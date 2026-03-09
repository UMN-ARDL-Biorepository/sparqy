import pandas as pd
from main import (
    extract_sampleid,
    flag_viable,
    redact_dsn_password,
    parquet_path,
    parse_sql_file,
)


def test_extract_sampleid():
    df = pd.DataFrame(
        {
            "COMMENTS": [
                "SAMPLEID:ABC-123, other text",
                "LAB_ID:999, more text",
                "Both SAMPLEID:XYZ-789, and LAB_ID:888",
                "Nothing here",
            ]
        }
    )
    result = extract_sampleid(df)
    assert result["SAMPLEID"][0] == "ABC-123"
    assert result["SAMPLEID2"][1] == "999"
    assert result["SAMPLEID"][2] == "XYZ-789"
    assert result["SAMPLEID2"][2] == "888"
    assert pd.isna(result["SAMPLEID"][3])
    assert pd.isna(result["SAMPLEID2"][3])


def test_flag_viable():
    df = pd.DataFrame(
        {
            "MATCODE": ["Box", None, "100x100Box", "Box", "Box", "Box"],
            "RECEIVED_CONDITION": ["Good", "Good", "Good", "SNR", "Good", "Good"],
            "SAMPLE_CONDITION": ["Good", "Good", "Good", "Good", "QNS", "Good"],
            "AMOUNTLEFT": [1.0, 1.0, 1.0, 1.0, 1.0, 0.0],
        }
    )
    exclude_conditions = ["SNR", "QNS"]
    exclude_matcodes = ["100x100Box"]

    result = flag_viable(df, exclude_conditions, exclude_matcodes)

    # Row 0: Valid
    assert result["VIABLE"][0]
    # Row 1: MATCODE is None
    assert not result["VIABLE"][1]
    # Row 2: Excluded MATCODE
    assert not result["VIABLE"][2]
    # Row 3: Excluded RECEIVED_CONDITION (SNR)
    assert not result["VIABLE"][3]
    # Row 4: Excluded SAMPLE_CONDITION (QNS)
    assert not result["VIABLE"][4]
    # Row 5: AMOUNTLEFT is 0
    assert not result["VIABLE"][5]


def test_redact_dsn_password():
    dsn = "Driver={ODBC Driver 17 for SQL Server};Server=myServer;Database=myDB;UID=myUser;PWD=secretPassword123;"
    redacted = redact_dsn_password(dsn)
    assert "PWD=****" in redacted
    assert "secretPassword123" not in redacted
    assert "UID=myUser" in redacted


def test_parquet_path(tmp_path):
    trial_code = "TRIAL-001"
    output_dir = tmp_path / "output"

    # Case 1: Simple path
    path = parquet_path(trial_code, output_dir, False, False)
    assert path.name == "TRIAL_001.parquet"
    assert path.parent == output_dir
    assert path.exists() is False  # Path returned, dirs created but file not yet

    # Case 2: Include DSN in filename
    path = parquet_path(trial_code, output_dir, True, False)
    assert "TRIAL_001_PROD" in path.name

    # Case 3: Add trial to path
    path = parquet_path(trial_code, output_dir, False, True)
    assert path.parent == output_dir / trial_code


def test_parse_sql_file(tmp_path):
    sql_content = "SELECT * FROM TABLE WHERE CODE = :trial_code"
    sql_file = tmp_path / "test.sql"
    sql_file.write_text(sql_content)

    query = parse_sql_file(sql_file)
    assert query == sql_content

    # Test non-existent file
    assert parse_sql_file(tmp_path / "missing.sql") is None
