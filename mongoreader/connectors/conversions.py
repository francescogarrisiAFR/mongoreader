"""This module contains functions to convert data from MongoDB to MMS
conventions and vice-versa."""

import re

# Chip stages

_chipStages_MongoDB = ['Chip testing', 'Final test']
_ChipStagesMMS = ['VFBCFIN', 'BCBCFIN']

CHIP_STAGES_MMS_to_MONGO = dict(zip(_ChipStagesMMS, _chipStages_MongoDB))
CHIP_STAGES_MONGO_to_MMS = dict(zip(_chipStages_MongoDB, _ChipStagesMMS))

ALL_WAFER_TYPES = ['BI', 'CA', 'CB', 'CDM', 'CM', 'CO', 'DR', 'DT', 'DY', 'EI']

# Dot Out Chip IDs

# --- Validiers ---

def isWaferNameValid(waferName:str) -> bool:

    rePatt = r'^\d{1}([A-Z]{2,3})\d{4}$'
    mtc = re.match(rePatt, waferName)

    if mtc is None: return False

    waferType = mtc.group(1)
    if waferType not in ALL_WAFER_TYPES: return False

    return True

# --- Converters ---

class Converter_dotOutChipID:

    def __init__(self):
        raise NotImplementedError('This class is not meant to be instantiated.')

    @classmethod
    def chipID_fromMongoName(cls, chipName:str) -> str:
        """Returns the chip ID from the chip name, which has to be composed of
        both the waferName and chip serial parts.

        Valid for all defined wafer types, except for DT, DY and EI wafers."""

        waferName, _ = chipName.split('_', maxsplit = 1)

        # Bilbao and CDM chips (SE/MZ/DF)
        if ('BI' in waferName or 'CDM' in waferName):
            return cls._chipID_fromMongoName_Bilbao(chipName)
        # Cordoba chips (COR L/V/M)
        elif 'CA' in waferName:
            return cls._chipID_fromMongoName_Cordoba(chipName)
        # Coimbra chips (COI QR/AM70)
        elif 'CB' in waferName:
            return cls._chipID_fromMongoName_Coimbra(chipName)
        # Cambridge chips (DR8)
        elif 'CM' in waferName:
            return cls._chipID_fromMongoName_default(chipName)
        # Como chips (FR8)
        elif 'CO' in waferName:
            return cls._chipID_fromMongoName_default(chipName)
        # Budapest chips (DR4/DR8/FR4/FR8)
        elif 'DR' in waferName:
            return cls._chipID_fromMongoName_default(chipName)
        # Dunkirk and Eindhoven chips (DT/DY/EI)
        elif ('DT' in waferName or 'DY' in waferName or 'EI' in waferName):
            if 'DT' in waferName:
                waferType = "Dunkirk B-type (DT)"
            elif 'DY' in waferName:
                waferType = "Dunkirk A-type (DY)"
            elif 'EI' in waferName:
                waferType = "Eindhoven (EI)"
            raise NotImplementedError(f'ChipID conversion is not supported for {waferType} wafers ("{waferName}").')
        else:
            raise NotImplementedError(f'Cannot generate dot out ChipID for wafer "{waferName}". Type not recognized.')

    @staticmethod
    def _chipID_fromMongoName_default(chipName:str) -> str:
        """Returns last two characters of the chip name, checking it is a number
        between 0 and 99."""
        _, chipSerial = chipName.split('_')

        ID = chipSerial.split('-')[-1] # Should be last two digits

        assert int(ID) <= 99
        assert int(ID) >= 0

        return ID
    
    def _chipID_fromMongoName_Bilbao(chipName:str) -> str:
        """Returns the chip ID from the chip name for Bilbao chips.

        rules:
            "x[BI/CDM]xxxx_NN-[SE/MZ/DF]" -> "NN"
        """

        _, chipSerial = chipName.split('_')

        chipID = chipSerial.split('-')[0]

        return chipID
    
    def _chipID_fromMongoName_Cordoba(chipName:str) -> str:
        """Depending on the wafer version, the chip name is either in the form
        "3CAxxxx_COR-yy-zz" or "3CAxxxx_Cxx-COR-yy", and has to be
        brought to "3CAxxxxww", where ww depends on yy and zz:
        
        rules:
            "COR-V1-01" -> "01" (V1 -> +0)
            "COR-V2-03" -> "33" (V2 -> +30)
            "COR-V3-06" -> "51" (V3 -> +45)
        """

        _, chipSerial = chipName.split('_')

        # Exception for COR-M1 chips
        if chipSerial in CORDOBA_M1_SERIALS_TO_CHIP_IDS:
            id = CORDOBA_M1_SERIALS_TO_CHIP_IDS[chipSerial]
            return f'{id:02}'

        _, V, N = chipSerial.split('-')
        N = int(N)
        Vnum = int(V[1])

        if not(V[0] == 'V' or V[0] == 'L'):
            raise NotImplementedError(f'Could not recognize the format of chip "{chipName}" for generating the Chip ID as a Cordoba chip.')

        if Vnum == 1:
            newN = N
        elif Vnum == 2:
            newN = N + 30
        elif Vnum == 3:
            newN = N + 45

        chipID = f'{newN:02}'
        return chipID
    
    def _chipID_fromMongoName_Coimbra(chipName:str) -> str:
        """Returns the chip ID from the chip name for Coimbra chips.

        rules:
            "3CBxxxx_COI-[QR/AM70]-yy" -> "yy"
        """

        _, chipSerial = chipName.split('_')

        chipID = chipSerial.split('-')[-1]

        return chipID

class Converter_dotOutDUTID:

    def __init__(self):
        raise NotImplementedError('This class is not meant to be instantiated.')
    
    @classmethod
    def DUT_ID_fromMongoName(cls, chipName:str) -> str:
        """Returns the DUT_ID string for the dot out file from the chip name,
        composed of both the waferName and chip serial parts."""

        waferName, _ = chipName.split('_', maxsplit = 1)
        return waferName + Converter_dotOutChipID.chipID_fromMongoName(chipName)


# Cordoba M1 serials to dot out chip IDs

CORDOBA_M1_SERIALS_TO_CHIP_IDS = {
    'A00-COR-M1': 0,
    'A01-COR-M1': 1,
    'A02-COR-M1': 2,
    'A03-COR-M1': 3,
    'A04-COR-M1': 4,
    'A05-COR-M1': 5,
    'A06-COR-M1': 6,
    'A07-COR-M1': 7,
    'B00-COR-M1': 8,
    'B01-COR-M1': 9,
    'B02-COR-M1': 10,
    'B03-COR-M1': 11,
    'B04-COR-M1': 12,
    'B05-COR-M1': 13,
    'B06-COR-M1': 14,
    'B07-COR-M1': 15,
    'B08-COR-M1': 16,
    'B09-COR-M1': 17,
    'C00-COR-M1': 18,
    'C01-COR-M1': 19,
    'C02-COR-M1': 20,
    'C03-COR-M1': 21,
    'C04-COR-M1': 22,
    'C05-COR-M1': 23,
    'C06-COR-M1': 24,
    'C07-COR-M1': 25,
    'C08-COR-M1': 26,
    'C09-COR-M1': 27,
    'D00-COR-M1': 28,
    'D01-COR-M1': 29,
    'D02-COR-M1': 30,
    'D03-COR-M1': 31,
    'D04-COR-M1': 32,
    'D05-COR-M1': 33,
    'D06-COR-M1': 34,
    'D07-COR-M1': 35,
    'D08-COR-M1': 36,
    'D09-COR-M1': 37,
    'E00-COR-M1': 38,
    'E01-COR-M1': 39,
    'E02-COR-M1': 40,
    'E03-COR-M1': 41,
    'E04-COR-M1': 42,
    'E05-COR-M1': 43,
    'E06-COR-M1': 44,
    'E07-COR-M1': 45,
    'E08-COR-M1': 46,
    'E09-COR-M1': 47,
    'F00-COR-M1': 48,
    'F01-COR-M1': 49,
    'F02-COR-M1': 50,
    'F03-COR-M1': 51,
    'F04-COR-M1': 52,
    'F05-COR-M1': 53,
    'F06-COR-M1': 54,
    'F07-COR-M1': 55
}

CORDOBA_CHIP_IDS_TO_M1_SERIALS = {v: k for k, v in CORDOBA_M1_SERIALS_TO_CHIP_IDS.items()}