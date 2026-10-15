import datetime as dt

import pytest


def clean(job, source, df):
    return job.CONFIGS[source]["clean"](df)


def one(df):
    rows = df.collect()
    assert len(rows) == 1
    return rows[0].asDict()


def ddl_columns(ddl):
    return [part.split()[0] for part in ddl.split(",") if part.strip()]


# --------------------------------------------------------------------------
# Config consistency (applies to every source)
# --------------------------------------------------------------------------
@pytest.mark.parametrize("source", ["dob", "acris", "pluto"])
def test_clean_output_columns_match_cassandra_ddl(job, make_raw, source):
    """INSERT statements are built from the cleaned columns, so they must line
    up exactly with the Cassandra table definition."""
    out = clean(job, source, make_raw(source, [{job.CONFIGS[source]["key"]: "k1"}]))
    assert sorted(out.columns) == sorted(ddl_columns(job.CONFIGS[source]["ddl"]))


@pytest.mark.parametrize("source", ["dob", "acris", "pluto"])
def test_key_is_a_raw_field_and_primary_key(job, source):
    cfg = job.CONFIGS[source]
    assert cfg["key"] in cfg["raw_fields"]
    assert f"{cfg['key']} text PRIMARY KEY" in " ".join(cfg["ddl"].split())


@pytest.mark.parametrize("source", ["dob", "acris", "pluto"])
def test_rows_with_null_key_are_dropped(job, make_raw, source):
    key = job.CONFIGS[source]["key"]
    df = make_raw(source, [{key: None}, {key: "keep-me"}])
    rows = clean(job, source, df).collect()
    assert [r[key] for r in rows] == ["keep-me"]


@pytest.mark.parametrize("source", ["dob", "acris", "pluto"])
def test_ingested_at_is_stamped(job, make_raw, source):
    key = job.CONFIGS[source]["key"]
    row = one(clean(job, source, make_raw(source, [{key: "k1"}])))
    assert row["ingested_at"] is not None


@pytest.mark.parametrize("source", ["dob", "acris", "pluto"])
def test_empty_batch_yields_empty_result(job, make_raw, source):
    assert clean(job, source, make_raw(source, [])).count() == 0


# --------------------------------------------------------------------------
# DOB permits
# --------------------------------------------------------------------------
class TestDob:
    RAW = {
        "permit_si_no": "123456",
        "job__": "B00001234-I1",
        "bin__": "1000001",
        "borough": "MANHATTAN",
        "house__": "10",
        "street_name": "BROADWAY",
        "zip_code": "10004",
        "permit_type": "NB",
        "permit_status": "ISSUED",
        "filing_status": "INITIAL",
        "filing_date": "03/15/2025",
        "job_start_date": "04/01/2025",
        "owner_s_business_name": "ACME REALTY LLC",
        "gis_latitude": "40.7061",
        "gis_longitude": "-74.0087",
        "gis_nta_name": "Financial District",
        "dobrundate": "2025-03-20T00:00:00.000",
    }

    def test_renames_columns(self, job, make_raw):
        row = one(clean(job, "dob", make_raw("dob", [self.RAW])))
        assert row["job_number"] == "B00001234-I1"
        assert row["bin"] == "1000001"
        assert row["house_number"] == "10"
        assert row["owner_business_name"] == "ACME REALTY LLC"
        assert row["neighborhood"] == "Financial District"

    def test_parses_us_style_dates(self, job, make_raw):
        row = one(clean(job, "dob", make_raw("dob", [self.RAW])))
        assert row["filing_date"] == dt.date(2025, 3, 15)
        assert row["job_start_date"] == dt.date(2025, 4, 1)

    def test_unparseable_date_becomes_null(self, job, make_raw):
        raw = {**self.RAW, "filing_date": "not-a-date", "job_start_date": None}
        row = one(clean(job, "dob", make_raw("dob", [raw])))
        assert row["filing_date"] is None
        assert row["job_start_date"] is None

    def test_casts_coordinates_to_double(self, job, make_raw):
        row = one(clean(job, "dob", make_raw("dob", [self.RAW])))
        assert row["latitude"] == pytest.approx(40.7061)
        assert row["longitude"] == pytest.approx(-74.0087)

    def test_bad_coordinates_become_null(self, job, make_raw):
        raw = {**self.RAW, "gis_latitude": "n/a", "gis_longitude": ""}
        row = one(clean(job, "dob", make_raw("dob", [raw])))
        assert row["latitude"] is None
        assert row["longitude"] is None

    def test_parses_run_date_timestamp(self, job, make_raw):
        row = one(clean(job, "dob", make_raw("dob", [self.RAW])))
        assert row["dob_run_date"] == dt.datetime(2025, 3, 20, 0, 0, 0)


# --------------------------------------------------------------------------
# ACRIS sales documents
# --------------------------------------------------------------------------
class TestAcris:
    RAW = {
        "document_id": "2025010100001",
        "recorded_borough": "3",
        "doc_type": "DEED",
        "document_date": "2025-01-15T00:00:00.000",
        "document_amt": "1250000",
        "recorded_datetime": "2025-01-20T10:30:00.000",
    }

    def test_renames_borough(self, job, make_raw):
        row = one(clean(job, "acris", make_raw("acris", [self.RAW])))
        assert row["borough"] == "3"
        assert "recorded_borough" not in row

    def test_document_amount_cast_to_double(self, job, make_raw):
        row = one(clean(job, "acris", make_raw("acris", [self.RAW])))
        assert row["document_amt"] == 1250000.0

    def test_non_numeric_amount_becomes_null(self, job, make_raw):
        raw = {**self.RAW, "document_amt": "TBD"}
        assert one(clean(job, "acris", make_raw("acris", [raw])))["document_amt"] is None

    def test_socrata_timestamp_strings_parse(self, job, make_raw):
        row = one(clean(job, "acris", make_raw("acris", [self.RAW])))
        assert row["document_date"] == dt.date(2025, 1, 15)
        assert row["recorded_datetime"] == dt.datetime(2025, 1, 20, 10, 30, 0)

    def test_duplicate_ids_are_not_collapsed_by_clean(self, job, make_raw):
        """Dedup is Cassandra's job (same primary key => upsert); clean() must
        not silently drop rows."""
        df = make_raw("acris", [self.RAW, self.RAW])
        assert clean(job, "acris", df).count() == 2


# --------------------------------------------------------------------------
# PLUTO parcels
# --------------------------------------------------------------------------
class TestPluto:
    RAW = {
        "bbl": "1000010010",
        "borough": "MN",
        "block": "1",
        "lot": "10",
        "address": "1 BROADWAY",
        "zipcode": "10004",
        "bldgclass": "O4",
        "landuse": "05",
        "ownername": "ACME REALTY LLC",
        "lotarea": "5000",
        "bldgarea": "20000.5",
        "numfloors": "4.5",
        "yearbuilt": "1925",
        "assessland": "1000000",
        "assesstot": "4500000",
        "latitude": "40.7",
        "longitude": "-74.0",
    }

    def test_renames_columns(self, job, make_raw):
        row = one(clean(job, "pluto", make_raw("pluto", [self.RAW])))
        assert row["zip_code"] == "10004"
        assert row["bldg_class"] == "O4"
        assert row["land_use"] == "05"
        assert row["owner_name"] == "ACME REALTY LLC"

    def test_numeric_casts(self, job, make_raw):
        row = one(clean(job, "pluto", make_raw("pluto", [self.RAW])))
        assert row["lot_area"] == 5000.0
        assert row["bldg_area"] == 20000.5
        assert row["num_floors"] == 4.5
        assert row["assess_land"] == 1000000.0
        assert row["assess_total"] == 4500000.0

    def test_year_built_is_int(self, job, make_raw):
        row = one(clean(job, "pluto", make_raw("pluto", [self.RAW])))
        assert row["year_built"] == 1925
        assert isinstance(row["year_built"], int)

    def test_blank_and_garbage_numerics_become_null(self, job, make_raw):
        raw = {**self.RAW, "lotarea": "", "yearbuilt": "unknown", "assesstot": "N/A"}
        row = one(clean(job, "pluto", make_raw("pluto", [raw])))
        assert row["lot_area"] is None
        assert row["year_built"] is None
        assert row["assess_total"] is None

    def test_leading_zero_land_use_is_preserved_as_text(self, job, make_raw):
        row = one(clean(job, "pluto", make_raw("pluto", [self.RAW])))
        assert row["land_use"] == "05"
