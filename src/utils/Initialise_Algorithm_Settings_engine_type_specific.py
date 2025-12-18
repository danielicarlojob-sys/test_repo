# previously named Read_Perf_Exchange_Rates_engine_type_specific
from src.utils.xrates_reader import Xrates_reader
from src.utils.log_file import LOG_FILE, log_message
import pandas as pd
import numpy as np


def Initialise_Algorithm_Settings_engine_type_specific() -> tuple[dict, dict]:
    """
    Extracts data from Xrate spreadsheet

    Returns
    ------
        - lim_dict, Xrates (dict, dict): dictionaries containing Xrates values and limits
    """

    # Parameters and thresholds

    Lag = [50, 100, 200, 400]
    nLag = len(Lag)
    RelErrThresh = [0.3]
    nRelErrThresh = len(RelErrThresh)
    EtaThresh = [0.2, 0.4, 0.6, 0.8, 1.0]
    nEtaThresh = len(EtaThresh)
    Param = [
        'PS26',
        'T25',
        'P30',
        'T30',
        'TGT',
        'NL',
        'NI',
        'NH',
        'FF',
        'P160']

    DSC = [52, 53, 54]  # CR, TO, CL
    nDSC = len(DSC)

    # Placeholder structure (would need actual implementation of
    # Read_Perf_Exchange_Rates_engine_type_specific)

    Xrates = dict()
    for d in range(nDSC):

        [flight_phase, df] = Xrates_reader(d)
        log_message(f"Xrates for DSC:{DSC[d]} extracted")
        Xrates[flight_phase] = df

    lim = 0.07
    num = 3
    BackDatingLimitDays = 14
    NumRawREALs = 31
    NumDerivedREALs = 140
    NumRawCELLs = 2
    NumDerivedCELLs = 0

    AltLowLim = [
        [25000],
        [-1000],
        [15000]
    ]
    AltUppLim = [
        [43000],
        [9000],
        [25000]
    ]

    MnLowLim = [
        [0.700],
        [0.175],
        [0.500]
    ]
    MnUppLim = [
        [0.880],
        [0.320],
        [0.770]
    ]

    ParamLowLim = [
        [25, 170, 130, 330, 380, 60, 75, 75, 3000],  # CR
        [50, 200, 400, 500, 650, 60, 75, 75, 10000],  # TO
        [50, 200, 250, 450, 650, 60, 75, 75, 8000]   # CL
    ]

    ParamUppLim = [
        [60, 280, 270, 590, 900, 120, 120, 120, 8500],  # CR
        [200, 400, 800, 800, 1000, 120, 120, 120, 25000],  # TO
        [100, 350, 500, 700, 900, 120, 120, 120, 16000]   # CL
    ]

    lim_item_list = [
        'WindowSemiWidth', 'MinSVdur', 'Lag',
        'nLag', 'RelErrThresh', 'nRelErrThresh',
                'EtaThresh', 'nEtaThresh',
                'lim', 'num', 'BackDatingLimitDays',
                'NumRawREALs', 'NumDerivedREALs',
                'NumRawCELLs', 'NumDerivedCELLs',
                'AltLowLim', 'AltUppLim',
                'MnLowLim', 'MnUppLim',
                'Param', 'ParamLowLim', 'ParamUppLim'
    ]
    lim_dict = {k: v for k, v in locals().items() if k in lim_item_list}
    log_message("All Xrates extracted")
    return lim_dict, Xrates


def Xrates_dic_vector_norm(Xrates_dict: dict) -> dict:
    """
    Filters Xrates DataFrames for each flight phase, keeping only relevant parameters,
    then calculates the norm for each Xrate

    Parameters
    ----------
    Args:
         - Xrates_dict (dict): input Xrates dictionary unfiltered

    Returns
    -------
         - Xrates_dict (dict): Xrates dictionary filtered, with Xrates Norms
    """

    parameters = ['P26', 'T26', 'P30', 'T30', 'TGT', 'NL', 'NI', 'NH', 'WFE']

    for flight_phase in Xrates_dict.keys():
        df = Xrates_dict[flight_phase].copy()
        df = df[parameters]
        df['Vector_Norm'] = np.linalg.norm(df.values, axis=1)
        Xrates_dict[flight_phase] = df

    return Xrates_dict

###########################################################################
# Manual test entry point
###########################################################################

if __name__ == "__main__":
    from pprint import pformat as pf
    lim_dict, Xrates = Initialise_Algorithm_Settings_engine_type_specific()
    
    log_message(f"lim_dict:\n{pf(lim_dict)}")
    # flight_phases = ['Climb', 'Cruise', 'Take-off']
    # parameters = ['P26', 'T26', 'P30', 'T30', 'TGT', 'NL', 'NI', 'NH', 'WFE']
    # [log_message(f"flight phase:{flight_phase}\n\n {Xrates_dic_vector_norm(Xrates)[flight_phase]}\n\n") for flight_phase in flight_phases]
    # [log_message(f"flight phase:{flight_phase}\n {Xrates[flight_phase][parameters].index}\n") for flight_phase in flight_phases]

    # log_message(Xrates)
