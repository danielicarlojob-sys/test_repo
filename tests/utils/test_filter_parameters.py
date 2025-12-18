import pandas as pd
import pytest
from src.utils.import_data_filters import filter_parameters


# ------------------------
# Fixtures: Take-off
# ------------------------
@pytest.fixture
def takeoff_valid():
    """Row inside limits for Take-off."""
    return pd.DataFrame([{
        "P25__PSI": 100,      # 50–200
        "T25__DEGC": 300,     # 200–400
        "P30__PSI": 600,      # 400–800
        "T30__DEGC": 650,     # 500–800
        "TGTU_A__DEGC": 800,  # 650–1000
        "NL__PC": 100,        # 60–120
        "NI__PC": 100,        # 75–120
        "NH__PC": 100,        # 75–120
        "FF__LBHR": 15000,    # 10000–25000
        "ALT__FT": 0,         # -1000–9000
        "MN1": 0.2            # 0.175–0.320
    }])


@pytest.fixture
def takeoff_invalid(takeoff_valid):
    """Row outside limits for Take-off (ALT way too high)."""
    df = takeoff_valid.copy()
    df.loc[0, "ALT__FT"] = 20000
    return df


# ------------------------
# Fixtures: Climb
# ------------------------
@pytest.fixture
def climb_valid():
    """Row inside limits for Climb."""
    return pd.DataFrame([{
        "P25__PSI": 80,       # 50–100
        "T25__DEGC": 300,     # 200–350
        "P30__PSI": 400,      # 250–500
        "T30__DEGC": 600,     # 450–700
        "TGTU_A__DEGC": 800,  # 650–900
        "NL__PC": 100,        # 60–120
        "NI__PC": 100,        # 75–120
        "NH__PC": 100,        # 75–120
        "FF__LBHR": 12000,    # 8000–16000
        "ALT__FT": 20000,     # 15000–25000
        "MN1": 0.6            # 0.5–0.77
    }])


@pytest.fixture
def climb_invalid(climb_valid):
    """Row outside limits for Climb (MN1 too low)."""
    df = climb_valid.copy()
    df.loc[0, "MN1"] = 0.1
    return df


# ------------------------
# Fixtures: Cruise
# ------------------------
@pytest.fixture
def cruise_valid():
    """Row inside limits for Cruise."""
    return pd.DataFrame([{
        "P25__PSI": 40,       # 25–60
        "T25__DEGC": 250,     # 170–280
        "P30__PSI": 200,      # 130–270
        "T30__DEGC": 500,     # 330–590
        "TGTU_A__DEGC": 600,  # 380–900
        "NL__PC": 100,        # 60–120
        "NI__PC": 100,        # 75–120
        "NH__PC": 100,        # 75–120
        "FF__LBHR": 5000,     # 3000–8500
        "ALT__FT": 30000,     # 25000–43000
        "MN1": 0.8            # 0.7–0.88
    }])


@pytest.fixture
def cruise_invalid(cruise_valid):
    """Row outside limits for Cruise (FF too high)."""
    df = cruise_valid.copy()
    df.loc[0, "FF__LBHR"] = 99999
    return df


# ------------------------
# Test classes
# ------------------------
class TestTakeoff:
    def test_valid_survives(self, takeoff_valid):
        df = filter_parameters(takeoff_valid, "Take-off")
        assert not df.empty

    def test_invalid_filtered(self, takeoff_invalid):
        df = filter_parameters(takeoff_invalid, "Take-off")
        assert df.empty

    def test_boundary_included(self, takeoff_valid):
        df = takeoff_valid.copy()
        df.loc[0, "MN1"] = 0.175  # exact low bound
        df2 = filter_parameters(df, "Take-off")
        assert not df2.empty


class TestClimb:
    def test_valid_survives(self, climb_valid):
        df = filter_parameters(climb_valid, "Climb")
        assert not df.empty

    def test_invalid_filtered(self, climb_invalid):
        df = filter_parameters(climb_invalid, "Climb")
        assert df.empty

    def test_mixed_rows_keep_valid(self, climb_valid):
        df = pd.concat([climb_valid, climb_valid], ignore_index=True)
        df.loc[1, "T25__DEGC"] = 9999
        df2 = filter_parameters(df, "Climb")
        assert len(df2) == 1


class TestCruise:
    def test_valid_survives(self, cruise_valid):
        df = filter_parameters(cruise_valid, "Cruise")
        assert not df.empty

    def test_invalid_filtered(self, cruise_invalid):
        df = filter_parameters(cruise_invalid, "Cruise")
        assert df.empty

    def test_boundary_included(self, cruise_valid):
        df = cruise_valid.copy()
        df.loc[0, "MN1"] = 0.88  # exact high bound
        df2 = filter_parameters(df, "Cruise")
        assert not df2.empty


class TestGeneral:

    @pytest.mark.parametrize("phase,fixture", [
        ("Take-off", "takeoff_valid"),
        ("Climb", "climb_valid"),
        ("Cruise", "cruise_valid"),
    ])
    def test_force_out_of_bounds(self, phase, fixture, request):
        df = request.getfixturevalue(fixture).copy()
        df.iloc[0, 0] = -99999  # wreck first param
        df2 = filter_parameters(df, phase)
        assert df2.empty
