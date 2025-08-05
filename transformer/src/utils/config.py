import re
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Database connection parameters
SOURCE_DB_HOST = os.getenv("SOURCE_DB_HOST", "localhost")
SOURCE_DB_USER = os.getenv("SOURCE_DB_USER", "root")
SOURCE_DB_PASSWORD = os.getenv("SOURCE_DB_PASSWORD", "")
SOURCE_DB_PORT = int(os.getenv("SOURCE_DB_PORT", 3306))
SOURCE_DB_NAME = os.getenv("SOURCE_MYSQL_DB_NAME")

DEST_DB_HOST = os.getenv("DEST_DB_HOST")
DEST_DB_USER = os.getenv("DEST_DB_USER")
DEST_DB_PASSWORD = os.getenv("DEST_DB_PASSWORD")
DEST_DB_PORT = int(os.getenv("DEST_DB_PORT", 3306))
DEST_DB_NAME_5MIN = os.getenv("DEST_MYSQL_DB_5MIN")
DEST_DB_NAME_15MIN = os.getenv("DEST_MYSQL_DB_15MIN")
DEST_DB_NAME_MGW = os.getenv("DEST_MYSQL_DB`_MGW")

# Source Database config
SOURCE_DB_CONFIG = {
    'host': SOURCE_DB_HOST,
    'user': SOURCE_DB_USER,
    'password': SOURCE_DB_PASSWORD,
    'port': SOURCE_DB_PORT,
    'database': SOURCE_DB_NAME
}

# Destination Database configs
DEST_DB_CONFIG_5MIN = {
    'host': DEST_DB_HOST,
    'user': DEST_DB_USER,
    'password': DEST_DB_PASSWORD,
    'port': DEST_DB_PORT,
    'database': DEST_DB_NAME_5MIN
}

DEST_DB_CONFIG_15MIN = {
    'host': DEST_DB_HOST,
    'user': DEST_DB_USER,
    'password': DEST_DB_PASSWORD,
    'port': DEST_DB_PORT,
    'database': DEST_DB_NAME_15MIN
}

DEST_DB_CONFIG_MGW = {
    'host': DEST_DB_HOST,
    'user': DEST_DB_USER,
    'password': DEST_DB_PASSWORD,
    'port': DEST_DB_PORT,
    'database': DEST_DB_NAME_MGW
}

# Node patterns
NOEUD_PATTERN = re.compile(r'^(CALIS|MEIND|RAIND)', re.IGNORECASE)
NOEUD_PATTERN_MGW = re.compile(r'^(MGW)', re.IGNORECASE)

# Files config
FILES_PATHS = {
    '5min': './data/our_data/result_5min.txt',
    '15min': './data/our_data/result_15min.txt',
    'mgw': './data/our_data/result_mgw.txt',
    'last_extracted': './data/last_extracted.json'
}

# Suffix to operator mapping
SUFFIX_OPERATOR_MAPPING = {
    'nw': 'Inwi',
    'mt': 'Maroc Telecom',
    'ie': 'International',
    'is': 'International',
    'bs': 'BSC 2G',
    'be': 'BSC 2G',
    'ne': 'RNC 3G',
    'ns': 'RNC 3G'
}

# Table definitions with KPIs for 5min
tables_5min = {
    'traffic_entree': {
        'kpis': {
            'traffic': {
                'numerator': ['VoiproITRALAC'],
                'formula': lambda num: sum(num)
            },
            'tentative_appel': {
                'numerator': ['VoiproNCALLSI'],
                'formula': lambda num: sum(num)
            },
            'appel_repondu': {
                'numerator': ['VoiproIANSWER'],
                'formula': lambda num: sum(num)
            },
            'appel_non_repondu': {
                'numerator': ['VoiproIOVERFL'],
                'formula': lambda num: sum(num)
            }
        }
    },
    'traffic_sortie': {
        'kpis': {
            'traffic': {
                'numerator': ['VoiproOTRALAC'],
                'formula': lambda num: sum(num)
            },
            'tentative_appel': {
                'numerator': ['VoiproNCALLSO'],
                'formula': lambda num: sum(num)
            },
            'appel_repondu': {
                'numerator': ['VoiproOANSWER'],
                'formula': lambda num: sum(num)
            },
            'appel_non_repondu': {
                'numerator': ['VoiproOOVERFL'],
                'formula': lambda num: sum(num)
            }
        }
    }
}

# Table definitions with KPIs for MGW
tables_mgw = {
    'mgw_kpis': {
        'kpis': {
            'RateOfLowJitterStream': {
                'numerator': [
                    'pmVoIpConnMeasuredJitter4',
                    'pmVoIpConnMeasuredJitter5',
                    'pmVoIpConnMeasuredJitter6',
                    'pmVoIpConnMeasuredJitter7',
                    'pmVoIpConnMeasuredJitter8'
                ],
                'denominator': [
                    'pmVoIpConnMeasuredJitter0',
                    'pmVoIpConnMeasuredJitter1',
                    'pmVoIpConnMeasuredJitter2',
                    'pmVoIpConnMeasuredJitter3',
                    'pmVoIpConnMeasuredJitter4',
                    'pmVoIpConnMeasuredJitter5',
                    'pmVoIpConnMeasuredJitter6',
                    'pmVoIpConnMeasuredJitter7',
                    'pmVoIpConnMeasuredJitter8'
                ],
                'formula': lambda num, denom: (1 - sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
            },
            'UseOfLicence': {
                'numerator': ['pmNrOfMeStChUsedVoip'],
                'denominator': ['maxNrOfLicMediaStreamChannelsVoip'],
                'formula': lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
            },
            'LatePktsRatio': {
                'numerator': [
                    'pmVoIpConnLatePktsRatio4',
                    'pmVoIpConnLatePktsRatio5',
                    'pmVoIpConnLatePktsRatio6'
                ],
                'denominator': [
                    'pmVoIpConnLatePktsRatio0',
                    'pmVoIpConnLatePktsRatio1',
                    'pmVoIpConnLatePktsRatio2',
                    'pmVoIpConnLatePktsRatio3',
                    'pmVoIpConnLatePktsRatio4',
                    'pmVoIpConnLatePktsRatio5',
                    'pmVoIpConnLatePktsRatio6'
                ],
                'formula': lambda num, denom: (1 - sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
            },
            'LatePktsVoIp': {
                'numerator': ['pmLatePktsVoIp'],
                'denominator': ['pmLatePktsVoIp', 'pmSuccTransmittedPktsVoIp'],
                'formula': lambda num, denom: sum(num) / sum(denom) if sum(denom) != 0 else None
            },
            'MediaStreamChannelUtilisationRate': {
                'numerator': ['pmNrOfMediaStreamChannelsBusy'],
                'denominator': ['maxNrOfLicMediaStreamChannels'],
                'formula': lambda num, denom: (sum(num) / sum(denom)) * 100 if sum(denom) != 0 else None
            },
            'IPQoS': {
                'numerator': [],
                'formula': lambda num: None
            },
            'PktLoss': {
                'numerator': ['pmRtpDiscardedPkts', 'pmRtpLostPkts'],
                'denominator': ['pmRtpReceivedPktsHi', 'pmRtpReceivedPktsLo', 'pmRtpLostPkts'],
                'formula': lambda num, denom: (sum(num) / ((denom[0] * 2147483648 + denom[1]) + denom[2])) * 100 if ((denom[0] * 2147483648 + denom[1]) + denom[2]) != 0 else None
            },
            'pmRtpReceivedPkts': {
                'numerator': ['pmRtpReceivedPktsHi', 'pmRtpReceivedPktsLo'],
                'formula': lambda num: (num[0] * 2147483648 + num[1])
            },
            'TotalBwForSig': {
                'numerator': ['pmSctpStatSentChunks', 'pmSctpStatRetransChunks'],
                'denominator': [],
                'formula': lambda num: (sum(num) / (1000000 * 900)) * 8 * 100 * 1.2
            },
            'NbIPTermination': {
                'numerator': ['pmNrOfIpTermsReq', 'pmNrOfIpTermsRej'],
                'formula': lambda num: num[0] - num[1]
            },
            'traffic_load': {
                'numerator': ['traffic_load'],
                'formula': lambda num: sum(num)
            }
        }
    }
}

# Collect counters for 5min
ALL_COUNTERS_5MIN = set()
for table_config in tables_5min.values():
    for kpi_config in table_config['kpis'].values():
        ALL_COUNTERS_5MIN.update(kpi_config.get('numerator', []) + kpi_config.get('denominator', []))
ALL_COUNTERS_5MIN = list(ALL_COUNTERS_5MIN)

# Collect counters for MGW
ALL_COUNTERS_MGW = set()
for table_config in tables_mgw.values():
    for kpi_config in table_config['kpis'].values():
        ALL_COUNTERS_MGW.update(kpi_config.get('numerator', []) + kpi_config.get('denominator', []))
ALL_COUNTERS_MGW = list(ALL_COUNTERS_MGW)

# Configuration dictionary
CONFIGS = {
    '5min': {
        'source_db_config': SOURCE_DB_CONFIG,
        'dest_db_config': DEST_DB_CONFIG_5MIN,
        'tables': tables_5min,
        'all_counters': ALL_COUNTERS_5MIN,
        'node_pattern': NOEUD_PATTERN,
        'suffix_operator_mapping': SUFFIX_OPERATOR_MAPPING,
        'file_path': FILES_PATHS['5min']
    }
}