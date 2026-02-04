import pandas as pd
import re

# =====================================================
# CONFIG
# =====================================================
INPUT_FILE = "output_addresses_fixed.xlsx"
OUTPUT_FILE = "final_address_output.xlsx"

ADDRESS_COL = "address"
LOCALITY_COL = "locality"

CONFIDENCE_COL = "locality_confidence"
FLAG_COL = "low_confidence_flag"
KEYWORDS_COL = "address_keywords"


# =====================================================
# DOMAIN CONFIG
# =====================================================
KEYWORD_WEIGHTS = {
    "SECTOR": 3, "PHASE": 3, "BLOCK": 3, "STAGE": 3,
    "COLONY": 2, "NAGAR": 2, "VIHAR": 2, "ENCLAVE": 2,
    "MARKET": 2, "PARK": 2,
    "ROAD": 1, "STREET": 1
}

SYNONYMS = {
    "SEC": "SECTOR",
    "SECT": "SECTOR",
    "RD": "ROAD",
    "ST": "STREET",
    "MKT": "MARKET",
    "E": "EAST", "PURVA": "EAST",
    "W": "WEST", "PASCHIM": "WEST",
    "N": "NORTH",
    "S": "SOUTH"
}

STOPWORDS = {
    "NEW", "OLD", "NEAR", "OPP", "OPPOSITE",
    "HOUSE", "HNO", "NO", "FLAT", "PLOT",
    "METRO", "GATE",
    "UTTAR", "PRADESH", "STATE", "INDIA",
    "PIN", "CODE", "DISTRICT", "HR", "HARYANA",
    "WEST", "EAST", "NORTH", "SOUTH"
}

CITY_CANONICAL = {
    "DELHI": "DELHI",
    "NEWDELHI": "DELHI",
    "GURUGRAM": "GURGAON",
    "GURGAON": "GURGAON",
    "NOIDA": "NOIDA",
    "BANGALORE": "BENGALURU",
    "BENGALURU": "BENGALURU",
    "MUMBAI": "MUMBAI",
    "BOMBAY": "MUMBAI"
}


# =====================================================
# NORMALIZATION
# =====================================================
def normalize_text(text):
    if pd.isna(text):
        return ""
    text = str(text).upper()
    text = re.sub(r"[^A-Z0-9 ]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_sector_token(token):
    return re.sub(r"(\d+)[A-Z]$", r"\1", token)


def tokenize(text):
    tokens = normalize_text(text).split()
    out = []
    for t in tokens:
        t = SYNONYMS.get(t, t)
        t = normalize_sector_token(t)
        out.append(t)
    return out


def extract_city(address):
    for c in CITY_CANONICAL:
        if c in address:
            return CITY_CANONICAL[c]
    return ""


def phonetic(token):
    return token[:2] if token else ""


# =====================================================
# CONFIDENCE SCORING
# =====================================================
def locality_confidence_score(address, locality):
    if not address or not locality:
        return 1

    addr_tokens = tokenize(address)
    loc_tokens = tokenize(locality)

    addr_set = set(addr_tokens)
    addr_ph = {phonetic(t) for t in addr_tokens}

    score = 0
    max_score = 0

    for t in loc_tokens:
        w = KEYWORD_WEIGHTS.get(t, 1)
        max_score += w
        if t in addr_set:
            score += w
        elif phonetic(t) in addr_ph:
            score += w * 0.5

    if max_score == 0:
        return 1

    ratio = score / max_score
    if ratio >= 0.85:
        return 10
    elif ratio >= 0.70:
        return 8
    elif ratio >= 0.50:
        return 6
    elif ratio >= 0.30:
        return 4
    else:
        return 2


# =====================================================
# KEYWORD EXTRACTION (USES LOCALITY + CONFIDENCE)
# =====================================================
def extract_address_keywords(address, locality, confidence):
    if confidence < 6:
        return ""

    addr_tokens = tokenize(address)
    loc_tokens = tokenize(locality)
    city = extract_city(normalize_text(address))

    loc_set = set(loc_tokens)
    matched = []

    i = 0
    while i < len(addr_tokens):
        token = addr_tokens[i]

        if token in STOPWORDS:
            i += 1
            continue

        # SECTOR + NUMBER
        if token == "SECTOR" and i + 1 < len(addr_tokens):
            if "SECTOR" in loc_set and addr_tokens[i + 1] in loc_set:
                matched.append(f"SECTOR {addr_tokens[i + 1]}")
                i += 2
                continue

        if token in loc_set:
            matched.append(token)

        i += 1

    # Deduplicate
    final = []
    seen = set()
    for m in matched:
        if m not in seen:
            seen.add(m)
            final.append(m)

    # Prefer AREA + SECTOR
    area = next((t for t in final if not t.startswith("SECTOR") and not t.isdigit()), None)
    sector = next((t for t in final if t.startswith("SECTOR")), None)

    result = []
    if area:
        result.append(area)
    if sector:
        result.append(sector)
    if city:
        result.append(city)

    return " ".join(result)


# =====================================================
# MAIN
# =====================================================
def main():
    print("Reading input...")
    df = pd.read_excel(INPUT_FILE)

    print("Calculating locality confidence...")
    df[CONFIDENCE_COL] = df.apply(
        lambda r: locality_confidence_score(
            r[ADDRESS_COL], r[LOCALITY_COL]
        ),
        axis=1
    )

    print("Extracting address keywords...")
    df[KEYWORDS_COL] = df.apply(
        lambda r: extract_address_keywords(
            r[ADDRESS_COL],
            r[LOCALITY_COL],
            r[CONFIDENCE_COL]
        ),
        axis=1
    )

    df[FLAG_COL] = df[CONFIDENCE_COL] <= 4

    print("Saving output...")
    df.to_excel(OUTPUT_FILE, index=False)

    print("Done.")
    print(df[CONFIDENCE_COL].value_counts().sort_index())


if __name__ == "__main__":
    main()
