"""Master data generator for RCD Corp: customers, products, employees, suppliers, stores, warehouses, fx_rates."""
from __future__ import annotations

from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker

from .base import BaseGenerator, MasterCache, ProfileConfig
from ..utils.distributions import normal_clipped, pareto_ltv, ltv_tier, weighted_choice
from ..utils.fx import generate_fx_rates
from ..utils.identifiers import generate_cpf, generate_cnpj, generate_sku, new_uuid
from ..utils.time_utils import random_date, COUNTRY_CURRENCY

LOCALES = ["pt_BR", "es_MX", "pt_PT", "en_US"]

COUNTRIES = ["BR", "MX", "PT", "US"]
COUNTRY_WEIGHTS = [0.60, 0.20, 0.12, 0.08]

SEGMENTS = ["B2C", "B2B", "VIP"]
SEGMENT_WEIGHTS = [0.75, 0.20, 0.05]

CHANNELS = ["web", "mobile_app", "in_store", "marketplace", "phone"]
CHANNEL_WEIGHTS = [0.35, 0.30, 0.15, 0.12, 0.08]

DEPARTMENTS = [
    "Engineering", "Product", "Data & Analytics", "Marketing", "Sales",
    "Customer Success", "Supply Chain", "Manufacturing", "Finance",
    "HR", "Legal", "IT", "Security",
]

EMPLOYMENT_TYPES = ["full_time", "part_time", "contractor"]
EMPLOYMENT_WEIGHTS = [0.80, 0.10, 0.10]

IC_LEVELS = ["IC1", "IC2", "IC3", "IC4", "IC5", "IC6", "IC7"]
MGMT_LEVELS = ["M1", "M2", "M3", "M4", "M5"]
ALL_LEVELS = IC_LEVELS + MGMT_LEVELS

SALARY_BY_LEVEL = {
    "IC1": (40_000, 8_000, 30_000, 55_000),
    "IC2": (55_000, 10_000, 42_000, 75_000),
    "IC3": (75_000, 12_000, 55_000, 100_000),
    "IC4": (100_000, 15_000, 75_000, 135_000),
    "IC5": (130_000, 20_000, 95_000, 180_000),
    "IC6": (165_000, 25_000, 120_000, 230_000),
    "IC7": (210_000, 30_000, 150_000, 300_000),
    "M1": (90_000, 15_000, 65_000, 125_000),
    "M2": (120_000, 20_000, 85_000, 165_000),
    "M3": (160_000, 25_000, 115_000, 215_000),
    "M4": (210_000, 35_000, 150_000, 300_000),
    "M5": (280_000, 45_000, 200_000, 400_000),
}

STORE_TYPES = ["flagship", "standard", "outlet", "pop_up", "online", "warehouse"]
STORE_TYPE_WEIGHTS = [0.02, 0.50, 0.25, 0.05, 0.10, 0.08]

WAREHOUSE_TYPES = ["central", "regional", "dark_store"]
WAREHOUSE_TYPE_WEIGHTS = [0.20, 0.50, 0.30]

SUPPLIER_PAYMENT_TERMS = ["net_30", "net_60", "net_90", "prepaid"]
SUPPLIER_CATEGORIES = ["electronics", "appliances", "packaging", "logistics", "components", "raw_materials"]

RCD_PRODUCTS = [
    ("NovaHome SmartPlug Mini", "NovaHome", "Smart Home", "Plugs", 89),
    ("NovaHome Thermostat Pro", "NovaHome", "Smart Home", "Climate", 799),
    ("NovaHome Hub X2", "NovaHome", "Smart Home", "Hubs", 1299),
    ("PulseAudio Earbuds Lite", "PulseAudio", "Audio", "Earbuds", 249),
    ("PulseAudio Soundbar 5.1", "PulseAudio", "Audio", "Soundbars", 2499),
    ("PulseAudio Studio Headphones", "PulseAudio", "Audio", "Headphones", 1899),
    ("GuardianIQ Cam 360", "GuardianIQ", "Security", "Cameras", 599),
    ("GuardianIQ Doorbell Pro", "GuardianIQ", "Security", "Doorbells", 899),
    ("GuardianIQ Alarm Kit", "GuardianIQ", "Security", "Alarms", 1499),
]

THIRD_PARTY_BRANDS = [
    ("Samsung", "Electronics", "TVs"),
    ("Samsung", "Electronics", "Phones"),
    ("LG", "Appliances", "Refrigerators"),
    ("LG", "Electronics", "TVs"),
    ("Sony", "Audio", "Headphones"),
    ("Sony", "Electronics", "Cameras"),
    ("Philips", "Appliances", "Small Appliances"),
    ("Philips", "Electronics", "Lighting"),
    ("Electrolux", "Appliances", "Washing Machines"),
    ("Electrolux", "Appliances", "Dishwashers"),
    ("Dell", "Computing", "Laptops"),
    ("Dell", "Computing", "Monitors"),
]

NAMED_STORES = [
    ("RCD Paulista", "BR", "São Paulo", "SP", "flagship"),
    ("RCD Ipanema", "BR", "Rio de Janeiro", "RJ", "flagship"),
    ("RCD Reforma", "MX", "Mexico City", "CDMX", "flagship"),
    ("RCD Chiado", "PT", "Lisbon", "LX", "flagship"),
    ("RCD Brickell", "US", "Miami", "FL", "flagship"),
]


class MasterDataGenerator(BaseGenerator):
    """Generates all master data tables for RCD Corp and populates MasterCache."""

    def generate(
        self,
        cache: MasterCache,
        profile: ProfileConfig,
        crisis_days: list[date],
    ) -> dict[str, pd.DataFrame]:
        fakers = {loc: Faker(loc) for loc in LOCALES}

        fx = generate_fx_rates(profile.start, profile.end, self.rng)
        suppliers = self._build_suppliers(profile, fakers)
        stores = self._build_stores(profile, fakers)
        warehouses = self._build_warehouses(profile, fakers)
        employees = self._build_employees(profile, fakers, stores)
        products = self._build_products(profile, suppliers)
        customers = self._build_customers(profile, fakers)

        return {
            "fx_rates": fx,
            "suppliers": suppliers,
            "stores": stores,
            "warehouses": warehouses,
            "employees": employees,
            "products": products,
            "customers": customers,
        }

    def _build_customers(self, profile: ProfileConfig, fakers: dict) -> pd.DataFrame:
        n = profile.n_customers
        countries = weighted_choice(COUNTRIES, COUNTRY_WEIGHTS, n, self.rng)
        segments = weighted_choice(SEGMENTS, SEGMENT_WEIGHTS, n, self.rng)
        channels = weighted_choice(CHANNELS, CHANNEL_WEIGHTS, n, self.rng)

        signup_start = date(2015, 1, 1)
        signup_end = profile.start

        ltv_vals = pareto_ltv(n, self.rng, scale=500)

        rows = []
        for i in range(n):
            country = countries[i]
            locale = {"BR": "pt_BR", "MX": "es_MX", "PT": "pt_PT", "US": "en_US"}[country]
            fk = fakers[locale]
            is_b2b = segments[i] == "B2B"
            signup = random_date(signup_start, signup_end, self.rng)
            rows.append({
                "id": new_uuid(),
                "name": fk.company() if is_b2b else fk.name(),
                "email": fk.email(),
                "phone": fk.phone_number(),
                "cpf_or_cnpj": generate_cnpj(self.rng) if is_b2b else generate_cpf(self.rng),
                "segment": segments[i],
                "country": country,
                "state": fk.state() if hasattr(fk, "state") else "",
                "city": fk.city(),
                "signup_date": signup,
                "ltv_tier": ltv_tier(ltv_vals)[i],
                "preferred_channel": channels[i],
                "loyalty_points": int(self.rng.integers(0, 50_000)),
            })
        return pd.DataFrame(rows)

    def _build_products(self, profile: ProfileConfig, suppliers: pd.DataFrame) -> pd.DataFrame:
        rows = []
        supplier_ids = suppliers["id"].tolist()
        idx = 0

        for name, brand, category, subcategory, price_brl in RCD_PRODUCTS:
            cost = round(price_brl * 0.45, 2)
            rows.append({
                "sku": generate_sku(brand[:3], idx),
                "name": name,
                "category": category,
                "subcategory": subcategory,
                "brand": brand,
                "cost": cost,
                "price": float(price_brl),
                "currency": "BRL",
                "margin": round((price_brl - cost) / price_brl, 4),
                "supplier_id": str(self.rng.choice(supplier_ids)),
                "weight_kg": round(float(self.rng.uniform(0.1, 5.0)), 2),
                "launch_date": date(2020, 1, 1) + timedelta(days=int(self.rng.integers(0, 365 * 4))),
                "is_active": True,
            })
            idx += 1

        n_third_party = min(profile.n_products - len(RCD_PRODUCTS), 200)
        for i in range(n_third_party):
            brand_cat = THIRD_PARTY_BRANDS[i % len(THIRD_PARTY_BRANDS)]
            brand, category, subcategory = brand_cat
            price = round(float(self.rng.uniform(150, 8000)), 2)
            cost = round(price * float(self.rng.uniform(0.40, 0.65)), 2)
            rows.append({
                "sku": generate_sku(brand[:3], idx),
                "name": f"{brand} {subcategory} {i + 1:03d}",
                "category": category,
                "subcategory": subcategory,
                "brand": brand,
                "cost": cost,
                "price": price,
                "currency": "BRL",
                "margin": round((price - cost) / price, 4),
                "supplier_id": str(self.rng.choice(supplier_ids)),
                "weight_kg": round(float(self.rng.uniform(0.1, 30.0)), 2),
                "launch_date": date(2018, 1, 1) + timedelta(days=int(self.rng.integers(0, 365 * 6))),
                "is_active": bool(self.rng.random() > 0.05),
            })
            idx += 1

        return pd.DataFrame(rows)

    def _build_employees(
        self, profile: ProfileConfig, fakers: dict, stores: pd.DataFrame
    ) -> pd.DataFrame:
        n = profile.n_employees
        store_locations = stores["city"].tolist()
        rows = []
        ids = [new_uuid() for _ in range(n)]

        for i in range(n):
            level = str(self.rng.choice(ALL_LEVELS))
            dept = str(self.rng.choice(DEPARTMENTS))
            mean, std, lo, hi = SALARY_BY_LEVEL[level]
            salary = float(normal_clipped(mean, std, lo, hi, 1, self.rng)[0])
            country = str(weighted_choice(COUNTRIES, COUNTRY_WEIGHTS, 1, self.rng)[0])
            locale = {"BR": "pt_BR", "MX": "es_MX", "PT": "pt_PT", "US": "en_US"}[country]
            fk = fakers[locale]
            # Manager: pick a random existing employee (or None for first few)
            manager_id = None if i < 5 else ids[int(self.rng.integers(0, max(i, 1)))]
            hire_start = date(2008, 1, 1)
            hire_end = profile.start
            hire_date = random_date(hire_start, hire_end, self.rng)
            rows.append({
                "id": ids[i],
                "name": fk.name(),
                "email": fk.email(),
                "department": dept,
                "role": f"{dept} Specialist" if level.startswith("IC") else f"{dept} Manager",
                "manager_id": manager_id,
                "hire_date": hire_date,
                "salary": round(salary, 2),
                "location": str(self.rng.choice(store_locations)) if store_locations else "São Paulo",
                "employment_type": str(weighted_choice(EMPLOYMENT_TYPES, EMPLOYMENT_WEIGHTS, 1, self.rng)[0]),
                "level": level,
                "country": country,
            })
        return pd.DataFrame(rows)

    def _build_suppliers(self, profile: ProfileConfig, fakers: dict) -> pd.DataFrame:
        rows = []
        for i in range(profile.n_suppliers):
            country = str(weighted_choice(COUNTRIES, COUNTRY_WEIGHTS, 1, self.rng)[0])
            locale = {"BR": "pt_BR", "MX": "es_MX", "PT": "pt_PT", "US": "en_US"}[country]
            fk = fakers[locale]
            rows.append({
                "id": new_uuid(),
                "name": fk.company(),
                "country": country,
                "rating": round(float(self.rng.uniform(1, 5)), 1),
                "lead_time_days": int(self.rng.integers(3, 60)),
                "payment_terms": str(self.rng.choice(SUPPLIER_PAYMENT_TERMS)),
                "category": str(self.rng.choice(SUPPLIER_CATEGORIES)),
            })
        return pd.DataFrame(rows)

    def _build_stores(self, profile: ProfileConfig, fakers: dict) -> pd.DataFrame:
        rows = []
        # Named flagship stores first
        for name, country, city, state, stype in NAMED_STORES:
            rows.append({
                "id": new_uuid(),
                "name": name,
                "region": state,
                "country": country,
                "city": city,
                "type": stype,
                "opening_date": date(2008 + len(rows), 3, 15),
                "size_sqm": int(self.rng.integers(1500, 5000)),
                "manager_employee_id": None,
            })
        # Fill remaining stores
        remaining = max(0, profile.n_stores - len(NAMED_STORES))
        for _ in range(remaining):
            country = str(weighted_choice(COUNTRIES, COUNTRY_WEIGHTS, 1, self.rng)[0])
            locale = {"BR": "pt_BR", "MX": "es_MX", "PT": "pt_PT", "US": "en_US"}[country]
            fk = fakers[locale]
            stype = str(weighted_choice(STORE_TYPES, STORE_TYPE_WEIGHTS, 1, self.rng)[0])
            open_year = int(self.rng.integers(2008, 2024))
            open_month = int(self.rng.integers(1, 13))
            rows.append({
                "id": new_uuid(),
                "name": f"RCD {fk.city()}",
                "region": fk.state() if hasattr(fk, "state") else "",
                "country": country,
                "city": fk.city(),
                "type": stype,
                "opening_date": date(open_year, open_month, 1),
                "size_sqm": int(self.rng.integers(200, 4000)),
                "manager_employee_id": None,
            })
        return pd.DataFrame(rows)

    def _build_warehouses(self, profile: ProfileConfig, fakers: dict) -> pd.DataFrame:
        rows = []
        wh_cities = [
            ("São Paulo", "BR"), ("Manaus", "BR"), ("Monterrey", "MX"),
            ("Lisbon", "PT"), ("Miami", "US"),
        ]
        for i in range(profile.n_warehouses):
            city, country = wh_cities[i % len(wh_cities)]
            wtype = str(weighted_choice(WAREHOUSE_TYPES, WAREHOUSE_TYPE_WEIGHTS, 1, self.rng)[0])
            rows.append({
                "id": new_uuid(),
                "name": f"RCD WH {city} {i + 1:02d}",
                "location": city,
                "country": country,
                "capacity_m3": int(self.rng.integers(1000, 50_000)),
                "manager_id": None,
                "type": wtype,
            })
        return pd.DataFrame(rows)
