"""Identifier generators with valid check digits for Aurora Corp."""
from __future__ import annotations

import string
import uuid

import numpy as np


def _cpf_check_digits(digits: list[int]) -> tuple[int, int]:
    total = sum(d * w for d, w in zip(digits[:9], range(10, 1, -1)))
    r = total % 11
    d1 = 0 if r < 2 else 11 - r

    total = sum(d * w for d, w in zip(digits[:9] + [d1], range(11, 1, -1)))
    r = total % 11
    d2 = 0 if r < 2 else 11 - r
    return d1, d2


def generate_cpf(rng: np.random.Generator) -> str:
    """Generate a valid Brazilian CPF with correct check digits."""
    digits = [int(rng.integers(0, 10)) for _ in range(9)]
    d1, d2 = _cpf_check_digits(digits)
    all_d = digits + [d1, d2]
    s = "".join(map(str, all_d))
    return f"{s[:3]}.{s[3:6]}.{s[6:9]}-{s[9:]}"


def _cnpj_check_digits(digits: list[int]) -> tuple[int, int]:
    w1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    total = sum(d * w for d, w in zip(digits[:12], w1))
    r = total % 11
    d1 = 0 if r < 2 else 11 - r

    w2 = [6, 5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
    total = sum(d * w for d, w in zip(digits[:12] + [d1], w2))
    r = total % 11
    d2 = 0 if r < 2 else 11 - r
    return d1, d2


def generate_cnpj(rng: np.random.Generator) -> str:
    """Generate a valid Brazilian CNPJ with correct check digits."""
    digits = [int(rng.integers(0, 10)) for _ in range(8)] + [0, 0, 0, 1]
    d1, d2 = _cnpj_check_digits(digits)
    all_d = digits + [d1, d2]
    s = "".join(map(str, all_d))
    return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:]}"


def generate_sku(category_prefix: str, index: int) -> str:
    """Generate a SKU: CAT-XXXXX."""
    prefix = category_prefix[:3].upper().replace(" ", "")
    return f"{prefix}-{index:05d}"


def generate_tracking_number(rng: np.random.Generator) -> str:
    """Generate a shipping tracking number (AU + 12 alphanumeric chars)."""
    chars = list(string.ascii_uppercase + string.digits)
    suffix = "".join(str(rng.choice(chars)) for _ in range(12))
    return f"AU{suffix}"


def new_uuid() -> str:
    return str(uuid.uuid4())


def generate_card_number(rng: np.random.Generator) -> str:
    """Generate a fake 16-digit card number (not Luhn-valid — synthetic only)."""
    digits = [str(int(rng.integers(0, 10))) for _ in range(16)]
    return f"{''.join(digits[:4])}-{''.join(digits[4:8])}-{''.join(digits[8:12])}-{''.join(digits[12:])}"
