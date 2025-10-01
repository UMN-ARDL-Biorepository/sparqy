import pandas as pd
import numpy as np

from .main import extract_sampleid


class Specimen:
    def __init__(self, comments):
        self.Comments = comments


class SpecimenDF:
    def __init__(self, specimens):
        self.df = pd.DataFrame([specimen.__dict__ for specimen in specimens])


def test_extract_sampleid():
    specimen_df = SpecimenDF(
        [Specimen("SAMPLEID:12345, Other info"), Specimen("No sample id here")]
    )
    print(specimen_df.df)
    extract_sampleid(specimen_df.df)
    assert specimen_df.df["SAMPLEID"].iloc[0] is not None
    assert specimen_df.df["SAMPLEID"].iloc[0] == "12345"
    assert specimen_df.df["SAMPLEID"].iloc[1] is np.nan
