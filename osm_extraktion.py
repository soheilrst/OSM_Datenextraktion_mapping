import requests
import pandas as pd
from typing import List, Dict
import re



german_states = [
    "Baden-Württemberg",
    "Bayern",
    "Berlin",
    "Brandenburg",
    "Bremen",
    "Hamburg",
    "Hessen",
    "Mecklenburg-Vorpommern",
    "Niedersachsen",
    "Nordrhein-Westfalen",
    "Rheinland-Pfalz",
    "Saarland",
    "Sachsen",
    "Sachsen-Anhalt",
    "Schleswig-Holstein",
    "Thüringen",
]

def fetch_osm_data(area: str = "Germany") -> pd.DataFrame:
    amenity_list = [
        "hotel",
        "bank",
        "supermarket",
        "company",
        "factory",
        "pharmacy",
        "hospital",
        "clinic",
        "dentist",
        "doctors",
        "kindergarten",
        "school",
        "college",
        "university",
        "library",
        "community_centre",
        "bicycle_rental",
        "car_rental",
        "car_sharing",
        "money_transfer",
        "post_office",
        "fire_station",
        "police",
        "courthouse",
        "embassy",
        "government",
        "insurance",
        "travel_agency",
        "conference_centre",
        "exhibition_centre",
        "language_school",
        "research_institute",
        "marketplace",
        "market",
    ]

    office_list = [
        "accountant",
        "advertising_agency",
        "airline",
        "architect",
        "association",
        "charity",
        "company",
        "construction_company",
        "consulting",
        "courier",
        "coworking",
        "diplomatic",
        "educational_institution",
        "	employment_agency",
        "energy_supplier",
        "engineer",
        "estate_agen",
        "financial",
        "financial_advisor",
        "foundation",
        "geodesist",
        "government",
        "graphic_design",
        "insurance",
        "it",
        "lawyer",
        "logistics",
        "moving_company",
        "newspaper",
        "ngo",
        "politician",
        "political_party",
        "property_management",
        "publisher",
        "religion" "research",
        "security",
        "surveyor",
        "tax_advisor",
        "telecommunication",
        "therapist",
        "travel_agent",
        "tutoring",
        "union",
        "visa",
        "yes",
    ]

    shop_list = [
        "department_store",
        "mall",
        "supermarket",
        "wholesale",
        "clothes",
        "jewelry",
        "watches",
        "charity",
        "variety_store",
        "beauty",
        "cosmetics",
        "hairdresser",
        "hearing_aids",
        "medical_supply",
        "perfumery",
        "appliance",
        "bathroom_furnishing",
        "electrical",
        "energy",
        "garden_furniture",
        "hardware",
        "security",
        "trade",
        "furniture",
        "computer",
        "electronics",
        "mobile_phone",
        "telecommunication",
        "bicycle",
        "car",
        "car_parts",
        "car_repair",
        "sports",
        "camera",
        "games",
        "music",
        "musical_instrument",
        "lottery",
        "copyshop",
        "rental",
        "travel_agency",
    ]

    overpass_url = "http://overpass-api.de/api/interpreter"
    amenity_regex = "|".join(amenity_list)
    office_regex = "|".join(office_list)
    shop_regex = "|".join(shop_list)

    query = f"""
    [out:json][timeout:10000];
    area["name"="{area}"]->.searchArea;
    (
      node["amenity"~"{amenity_regex}",i](area.searchArea);
      way["amenity"~"{amenity_regex}",i](area.searchArea);
      node["brand"~"{amenity_regex}",i](area.searchArea);
      way["brand"~"{amenity_regex}",i](area.searchArea);
      node["operator"~"{amenity_regex}",i](area.searchArea);
      way["operator"~"{amenity_regex}",i](area.searchArea);
      node["office"~"{office_regex}",i](area.searchArea);
      way["office"~"{office_regex}",i](area.searchArea);
      node["shop"~"{shop_regex}",i](area.searchArea);
      way["shop"~"{shop_regex}",i](area.searchArea);
    );
    out body;
    >;
    out skel qt;
    """

    try:
        response = requests.get(overpass_url, params={"data": query})
        response.raise_for_status()
        if response.content.strip():
            data = response.json()
        else:
            raise ValueError("Empty response from Overpass API")
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        return pd.DataFrame()
    except ValueError as e:
        print(f"Value error: {e}")
        return pd.DataFrame()

    elements = data["elements"]
    company_data = []

    for element in elements:
        tags = element.get("tags", {})
        amenity = tags.get("amenity", None)
        operator = tags.get("operator", None)
        brand = tags.get("brand", None)
        office = tags.get("office", None)
        shop = tags.get("shop", None)
        name = tags.get("name", None)
        street = tags.get("addr:street", None)
        housenumber = tags.get("addr:housenumber", None)
        postcode = tags.get("addr:postcode", None)
        city = tags.get("addr:city", None)

        lat, lon = None, None
        if "lat" in element and "lon" in element:
            lat = element["lat"]
            lon = element["lon"]
        elif "center" in element:
            lat = element["center"]["lat"]
            lon = element["center"]["lon"]

        if name and street and housenumber and postcode and city:
            company_data.append(
                {
                    "COMPANYNAME": name,
                    "AMENITY": amenity,
                    "OPERATOR": operator,
                    "BRAND": brand,
                    "OFFICE": office,
                    "SHOP": shop,
                    "CITY": city,
                    "POSTALCODE": postcode,
                    "STREET": street,
                    "HSNR": housenumber,
                    "LAT": lat,
                    "LON": lon,
                }
            )

    df = pd.DataFrame(company_data).drop_duplicates(
        subset=["COMPANYNAME", "OPERATOR", "STREET", "HSNR"], keep="first"
    )
    return df



def save_data_by_state(states: List[str]):
    for state in states:
        print(f"Fetching data for {state}...")
        df = fetch_osm_data(state)
        if not df.empty:
            file_name = f"OSM_{state}.parquet"
            df.to_parquet(file_name, index=False)
            print(f"Data for {state} saved to {file_name}")
        else:
            print(f"No data found for {state}")


def read_and_concat_parquet_files(states: List[str]):
    df_osm = pd.DataFrame()

    for state in german_states:
        file_name = """OSM_{state}.parquet"""
        
        
        if os.path.exists(file_name):
            df_state = pd.read_parquet(file_name)
            df_state["STATE"] = state
            df_osm = pd.concat([df_osm, df_state], ignore_index=True)
        else:
            print(f"File {file_name} does not exist.")
    
    return df_osm