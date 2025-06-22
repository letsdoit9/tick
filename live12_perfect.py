import streamlit as st
import pandas as pd
import requests
import numpy as np
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import ta
import time
import multiprocessing
from io import StringIO
import json
import websocket
import threading
from streamlit_autorefresh import st_autorefresh

# HARDCODED STOCK LIST
DEFAULT_STOCKS = """instrument_key,tradingsymbol
NSE_EQ|INE585B01010,MARUTI
NSE_EQ|INE139A01034,NATIONALUM
NSE_EQ|INE763I01026,TARIL
NSE_EQ|INE970X01018,LEMONTREE
NSE_EQ|INE522D01027,MANAPPURAM
NSE_EQ|INE427F01016,CHALET
NSE_EQ|INE00R701025,DALBHARAT
NSE_EQ|INE917I01010,BAJAJ-AUTO
NSE_EQ|INE146L01010,KIRLOSENG
NSE_EQ|INE267A01025,HINDZINC
NSE_EQ|INE466L01038,360ONE
NSE_EQ|INE070A01015,SHREECEM
NSE_EQ|INE242C01024,ANANTRAJ
NSE_EQ|INE883F01010,AADHARHFC
NSE_EQ|INE749A01030,JINDALSTEL
NSE_EQ|INE171Z01026,BDL
NSE_EQ|INE591G01017,COFORGE
NSE_EQ|INE903U01023,SIGNATURE
NSE_EQ|INE160A01022,PNB
NSE_EQ|INE640A01023,SKFINDIA
NSE_EQ|INE814H01011,ADANIPOWER
NSE_EQ|INE736A01011,CDSL
NSE_EQ|INE301A01014,RAYMOND
NSE_EQ|INE102D01028,GODREJCP
NSE_EQ|INE600L01024,LALPATHLAB
NSE_EQ|INE134E01011,PFC
NSE_EQ|INE269A01021,SONATSOFTW
NSE_EQ|INE009A01021,INFY
NSE_EQ|INE962Y01021,IRCON
NSE_EQ|INE048G01026,NAVINFLUOR
NSE_EQ|INE918Z01012,KAYNES
NSE_EQ|INE376G01013,BIOCON
NSE_EQ|INE00M201021,SWSOLAR
NSE_EQ|INE619A01035,PATANJALI
NSE_EQ|INE465A01025,BHARATFORG
NSE_EQ|INE589A01014,NLCINDIA
NSE_EQ|INE463A01038,BERGEPAINT
NSE_EQ|INE622W01025,ACMESOLAR
NSE_EQ|INE256A01028,ZEEL
NSE_EQ|INE540L01014,ALKEM
NSE_EQ|INE237A01028,KOTAKBANK
NSE_EQ|INE126A01031,EIDPARRY
NSE_EQ|INE482A01020,CEATLTD
NSE_EQ|INE850D01014,GODREJAGRO
NSE_EQ|INE361B01024,DIVISLAB
NSE_EQ|INE517B01013,TTML
NSE_EQ|INE385C01021,SARDAEN
NSE_EQ|INE811K01011,PRESTIGE
NSE_EQ|INE01EA01019,VMM
NSE_EQ|INE510A01028,ENGINERSIN
NSE_EQ|INE030A01027,HINDUNILVR
NSE_EQ|INE872J01023,DEVYANI
NSE_EQ|INE476A01022,CANBK
NSE_EQ|INE419U01012,HAPPSTMNDS
NSE_EQ|INE691A01018,UCOBANK
NSE_EQ|INE745G01035,MCX
NSE_EQ|INE0W2G01015,SAGILITY
NSE_EQ|INE531E01026,HINDCOPPER
NSE_EQ|INE483C01032,TANLA
NSE_EQ|INE721A01047,SHRIRAMFIN
NSE_EQ|INE028A01039,BANKBARODA
NSE_EQ|INE670K01029,LODHA
NSE_EQ|INE039A01010,IFCI
NSE_EQ|INE914M01019,ASTERDM
NSE_EQ|INE570L01029,SAILIFE
NSE_EQ|INE158A01026,HEROMOTOCO
NSE_EQ|INE112L01020,METROPOLIS
NSE_EQ|INE405E01023,UNOMINDA
NSE_EQ|INE777K01022,RRKABEL
NSE_EQ|INE123W01016,SBILIFE
NSE_EQ|INE192A01025,TATACONSUM
NSE_EQ|INE398R01022,SYNGENE
NSE_EQ|INE118A01012,BAJAJHLDNG
NSE_EQ|INE371A01025,GRAPHITE
NSE_EQ|INE373A01013,BASF
NSE_EQ|INE674K01013,ABCAPITAL
NSE_EQ|INE094A01015,HINDPETRO
NSE_EQ|INE410P01011,NH
NSE_EQ|INE203A01020,ASTRAZEN
NSE_EQ|INE528G01035,YESBANK
NSE_EQ|INE248A01017,ITI
NSE_EQ|INE531F01015,NUVAMA
NSE_EQ|INE093I01010,OBEROIRLTY
NSE_EQ|INE616N01034,INOXINDIA
NSE_EQ|INE726G01019,ICICIPRULI
NSE_EQ|INE901L01018,APLLTD
NSE_EQ|INE271B01025,MAHSEAMLES
NSE_EQ|INE073K01018,SONACOMS
NSE_EQ|INE006I01046,ASTRAL
NSE_EQ|INE142M01025,TATATECH
NSE_EQ|INE036D01028,KARURVYSYA
NSE_EQ|INE885A01032,ARE&M
NSE_EQ|INE233B01017,BLUEDART
NSE_EQ|INE169A01031,COROMANDEL
NSE_EQ|INE235A01022,FINCABLES
NSE_EQ|INE668F01031,JYOTHYLAB
NSE_EQ|INE849A01020,TRENT
NSE_EQ|INE669C01036,TECHM
NSE_EQ|INE322A01010,GILLETTE
NSE_EQ|INE216A01030,BRITANNIA
NSE_EQ|INE002S01010,MGL
NSE_EQ|INE111A01025,CONCOR
NSE_EQ|INE531A01024,KANSAINER
NSE_EQ|INE062A01020,SBIN
NSE_EQ|INE180C01042,CGCL
NSE_EQ|INE128S01021,FIVESTAR
NSE_EQ|INE672A01018,TATAINVEST
NSE_EQ|INE216P01012,AAVAS
NSE_EQ|INE220B01022,KPIL
NSE_EQ|INE081A01020,TATASTEEL
NSE_EQ|INE007A01025,CRISIL
NSE_EQ|INE883A01011,MRF
NSE_EQ|INE824G01012,JSWHL
NSE_EQ|INE075A01022,WIPRO
NSE_EQ|INE498L01015,LTF
NSE_EQ|INE377N01017,WAAREEENER
NSE_EQ|INE484J01027,GODREJPROP
NSE_EQ|INE979A01025,SAREGAMA
NSE_EQ|INE188A01015,FACT
NSE_EQ|INE205A01025,VEDL
NSE_EQ|INE027H01010,MAXHEALTH
NSE_EQ|INE298J01013,NAM-INDIA
NSE_EQ|INE101D01020,GRANULES
NSE_EQ|INE212H01026,AIAENG
NSE_EQ|INE967H01025,KIMS
NSE_EQ|INE121A01024,CHOLAFIN
NSE_EQ|INE010J01012,TEJASNET
NSE_EQ|INE474Q01031,MEDANTA
NSE_EQ|INE839M01018,SCHNEIDER
NSE_EQ|INE074A01025,PRAJIND
NSE_EQ|INE974X01010,TIINDIA
NSE_EQ|INE854D01024,UNITDSPR
NSE_EQ|INE220G01021,JSL
NSE_EQ|INE742F01042,ADANIPORTS
NSE_EQ|INE226A01021,VOLTAS
NSE_EQ|INE0NT901020,NETWEB
NSE_EQ|INE292B01021,HBLENGINE
NSE_EQ|INE047A01021,GRASIM
NSE_EQ|INE326A01037,LUPIN
NSE_EQ|INE584A01023,NMDC
NSE_EQ|INE085A01013,CHAMBLFERT
NSE_EQ|INE03Q201024,ALIVUS
NSE_EQ|INE836A01035,BSOFT
NSE_EQ|INE548A01028,HFCL
NSE_EQ|INE501A01019,DEEPAKFERT
NSE_EQ|INE414G01012,MUTHOOTFIN
NSE_EQ|INE669E01016,IDEA
NSE_EQ|INE743M01012,RHIM
NSE_EQ|INE324A01032,JINDALSAW
NSE_EQ|INE211B01039,PHOENIXLTD
NSE_EQ|INE813H01021,TORNTPOWER
NSE_EQ|INE066P01011,INOXWIND
NSE_EQ|INE880J01026,JSWINFRA
NSE_EQ|INE358A01014,ABBOTINDIA
NSE_EQ|INE868B01028,NCC
NSE_EQ|INE172A01027,CASTROLIND
NSE_EQ|INE213A01029,ONGC
NSE_EQ|INE825A01020,VTL
NSE_EQ|INE0FS801015,MSUMI
NSE_EQ|INE335Y01020,IRCTC
NSE_EQ|INE406M01024,ERIS
NSE_EQ|INE725A01030,NAVA
NSE_EQ|INE00WC01027,AFFLE
NSE_EQ|INE931S01010,ADANIENSOL
NSE_EQ|INE704P01025,COCHINSHIP
NSE_EQ|INE053F01010,IRFC
NSE_EQ|INE127D01025,HDFCAMC
NSE_EQ|INE021A01026,ASIANPAINT
NSE_EQ|INE671A01010,HONAUT
NSE_EQ|INE356A01018,MPHASIS
NSE_EQ|INE571A01038,IPCALAB
NSE_EQ|INE733E01010,NTPC
NSE_EQ|INE230A01023,EIHOTEL
NSE_EQ|INE565A01014,IOB
NSE_EQ|INE022Q01020,IEX
NSE_EQ|INE115A01026,LICHSGFIN
NSE_EQ|INE475E01026,CAPLIPOINT
NSE_EQ|INE463V01026,ANANDRATHI
NSE_EQ|INE596I01012,CAMS
NSE_EQ|INE684F01012,FSL
NSE_EQ|INE702C01027,APLAPOLLO
NSE_EQ|INE017A01032,GESHIP
NSE_EQ|INE388Y01029,NYKAA
NSE_EQ|INE348B01021,CENTURYPLY
NSE_EQ|INE117A01022,ABB
NSE_EQ|INE239A01024,NESTLEIND
NSE_EQ|INE02ID01020,RAYMONDLSL
NSE_EQ|INE980O01024,JYOTICNC
NSE_EQ|INE228A01035,USHAMART
NSE_EQ|INE437A01024,APOLLOHOSP
NSE_EQ|INE245A01021,TATAPOWER
NSE_EQ|INE288B01029,DEEPAKNTR
NSE_EQ|INE053A01029,INDHOTEL
NSE_EQ|INE927D01051,JBMA
NSE_EQ|INE995S01015,NIVABUPA
NSE_EQ|INE100A01010,ATUL
NSE_EQ|INE665A01038,SWANENERGY
NSE_EQ|INE196A01026,MARICO
NSE_EQ|INE338H01029,CONCORDBIO
NSE_EQ|INE152M01016,TRITURBINE
NSE_EQ|INE121J01017,INDUSTOWER
NSE_EQ|INE140A01024,PEL
NSE_EQ|INE389H01022,KEC
NSE_EQ|INE399L01023,ATGL
NSE_EQ|INE055A01016,ABREL
NSE_EQ|INE024L01027,GRAVITA
NSE_EQ|INE615H01020,TITAGARH
NSE_EQ|INE121E01018,JSWENERGY
NSE_EQ|INE019A01038,JSWSTEEL
NSE_EQ|INE0IX101010,DATAPATTNS
NSE_EQ|INE450U01017,ROUTE
NSE_EQ|INE151A01013,TATACOMM
NSE_EQ|INE522F01014,COALINDIA
NSE_EQ|INE382Z01011,GRSE
NSE_EQ|INE095N01031,NBCC
NSE_EQ|INE296A01024,BAJFINANCE
NSE_EQ|INE066F01020,HAL
NSE_EQ|INE002A01018,RELIANCE
NSE_EQ|INE462A01022,BAYERCROP
NSE_EQ|INE961O01016,RAINBOW
NSE_EQ|INE203G01027,IGL
NSE_EQ|INE619B01017,NEWGEN
NSE_EQ|INE109A01011,SCI
NSE_EQ|INE183A01024,FINPIPE
NSE_EQ|INE113A01013,GNFC
NSE_EQ|INE467B01029,TCS
NSE_EQ|INE573A01042,JKTYRE
NSE_EQ|INE806T01020,SAPPHIRE
NSE_EQ|INE473A01011,LINDEINDIA
NSE_EQ|INE153T01027,BLS
NSE_EQ|INE258A01016,BEML
NSE_EQ|INE759A01021,MASTEK
NSE_EQ|INE0ONG01011,NTPCGREEN
NSE_EQ|INE149A01033,CHOLAHLDNG
NSE_EQ|INE192B01031,WELSPUNLIV
NSE_EQ|INE079A01024,AMBUJACEM
NSE_EQ|INE457L01029,PGEL
NSE_EQ|INE0J1Y01017,LICI
NSE_EQ|INE260B01028,GODFRYPHLP
NSE_EQ|INE299U01018,CROMPTON
NSE_EQ|INE040A01034,HDFCBANK
NSE_EQ|INE200A01026,GVT&D
NSE_EQ|INE121A08PJ0,CHOLAFIN
NSE_EQ|INE270A01029,ALOKINDS
NSE_EQ|INE371P01015,AMBER
NSE_EQ|INE205B01031,ELECON
NSE_EQ|INE486A01021,CESC
NSE_EQ|INE399G01023,RKFORGE
NSE_EQ|INE603J01030,PIIND
NSE_EQ|INE202E01016,IREDA
NSE_EQ|INE663F01032,NAUKRI
NSE_EQ|INE066A01021,EICHERMOT
NSE_EQ|INE844O01030,GUJGASLTD
NSE_EQ|INE481N01025,HOMEFIRST
NSE_EQ|INE421D01022,CCL
NSE_EQ|INE752E01010,POWERGRID
NSE_EQ|INE271C01023,DLF
NSE_EQ|INE318A01026,PIDILITIND
NSE_EQ|INE208C01025,AEGISLOG
NSE_EQ|INE520A01027,ZENSARTECH
NSE_EQ|INE818H01020,LTFOODS
NSE_EQ|INE499A01024,DCMSHRIRAM
NSE_EQ|INE306R01017,INTELLECT
NSE_EQ|INE042A01014,ESCORTS
NSE_EQ|INE176A01028,BATAINDIA
NSE_EQ|INE064C01022,TRIDENT
NSE_EQ|INE285K01026,TECHNOE
NSE_EQ|INE256C01024,TRIVENI
NSE_EQ|INE274F01020,WESTLIFE
NSE_EQ|INE947Q01028,LAURUSLABS
NSE_EQ|INE913H01037,ENDURANCE
NSE_EQ|INE918I01026,BAJAJFINSV
NSE_EQ|INE758E01017,JIOFIN
NSE_EQ|INE089A01031,DRREDDY
NSE_EQ|INE251B01027,ZENTEC
NSE_EQ|INE575P01011,STARHEALTH
NSE_EQ|INE195J01029,PNCINFRA
NSE_EQ|INE834M01019,RTNINDIA
NSE_EQ|INE848E01016,NHPC
NSE_EQ|INE852O01025,APTUS
NSE_EQ|INE545A01024,HEG
NSE_EQ|INE982J01020,PAYTM
NSE_EQ|INE205C01021,POLYMED
NSE_EQ|INE634I01029,KNRCON
NSE_EQ|INE761H01022,PAGEIND
NSE_EQ|INE342J01019,ZFCVINDIA
NSE_EQ|INE494B01023,TVSMOTOR
NSE_EQ|INE673O01025,TBOTEK
NSE_EQ|INE646L01027,INDIGO
NSE_EQ|INE0V6F01027,HYUNDAI
NSE_EQ|INE010B01027,ZYDUSLIFE
NSE_EQ|INE302A01020,EXIDEIND
NSE_EQ|INE0BY001018,JUBLINGREA
NSE_EQ|INE810G01011,SHYAMMETL
NSE_EQ|INE351F01018,JPPOWER
NSE_EQ|INE634S01028,MANKIND
NSE_EQ|INE191B01025,WELCORP
NSE_EQ|INE397D01024,BHARTIARTL
NSE_EQ|INE192R01011,DMART
NSE_EQ|INE686F01025,UBL
NSE_EQ|INE123F01029,MMTC
NSE_EQ|INE008A01015,IDBI
NSE_EQ|INE321T01012,DOMS
NSE_EQ|INE775A08105,MOTHERSON
NSE_EQ|INE933S01016,INDIAMART
NSE_EQ|INE732I01013,ANGELONE
NSE_EQ|INE059A01026,CIPLA
NSE_EQ|INE00E101023,BIKAJI
NSE_EQ|INE660A01013,SUNDARMFIN
NSE_EQ|INE03QK01018,COHANCE
NSE_EQ|INE138Y01010,KFINTECH
NSE_EQ|INE377Y01014,BAJAJHFL
NSE_EQ|INE168P01015,EMCURE
NSE_EQ|INE343G01021,BHARTIHEXA
NSE_EQ|INE481Y01014,GICRE
NSE_EQ|INE797F01020,JUBLFOOD
NSE_EQ|INE180A01020,MFSL
NSE_EQ|INE949L01017,AUBANK
NSE_EQ|INE881D01027,OFSS
NSE_EQ|INE795G01014,HDFCLIFE
NSE_EQ|INE439A01020,ASAHIINDIA
NSE_EQ|INE148I01020,SAMMAANCAP
NSE_EQ|INE823G01014,JKCEMENT
NSE_EQ|INE987B01026,NATCOPHARM
NSE_EQ|INE280A01028,TITAN
NSE_EQ|INE227W01023,CLEAN
NSE_EQ|INE716A01013,WHIRLPOOL
NSE_EQ|INE03JT01014,GODIGIT
NSE_EQ|INE298A01020,CUMMINSIND
NSE_EQ|INE470Y01017,NIACL
NSE_EQ|INE769A01020,AARTIIND
NSE_EQ|INE155A01022,TATAMOTORS
NSE_EQ|INE119A01028,BALRAMCHIN
NSE_EQ|INE258G01013,SUMICHEM
NSE_EQ|INE930H01031,KPRMILL
NSE_EQ|INE614G01033,RPOWER
NSE_EQ|INE274J01014,OIL
NSE_EQ|INE372A01015,APARINDS
NSE_EQ|INE02RE01045,FIRSTCRY
NSE_EQ|INE285A01027,ELGIEQUIP
NSE_EQ|INE383A01012,INDIACEM
NSE_EQ|INE012A01025,ACC
NSE_EQ|INE0NNS01018,NSLNISP
NSE_EQ|INE944F01028,RADICO
NSE_EQ|INE572E01012,PNBHOUSING
NSE_EQ|INE281B01032,LLOYDSME
NSE_EQ|INE050A01025,BBTC
NSE_EQ|INE095A01012,INDUSINDBK
NSE_EQ|INE09N301011,FLUOROCHEM
NSE_EQ|INE513A01022,SCHAEFFLER
NSE_EQ|INE562A01011,INDIANB
NSE_EQ|INE780C01023,JMFINANCIL
NSE_EQ|INE195A01028,SUPREMEIND
NSE_EQ|INE049B01025,WOCKPHARMA
NSE_EQ|INE483A01010,CENTRALBK
NSE_EQ|INE136B01020,CYIENT
NSE_EQ|INE043W01024,VIJAYA
NSE_EQ|INE209L01016,JWL
NSE_EQ|INE168A01041,J&KBANK
NSE_EQ|INE870H01013,NETWORK18
NSE_EQ|INE118H01025,BSE
NSE_EQ|INE364U01010,ADANIGREEN
NSE_EQ|INE101I01011,AFCONS
NSE_EQ|INE238A01034,AXISBANK
NSE_EQ|INE065X01017,INDGN
NSE_EQ|INE044A01036,SUNPHARMA
NSE_EQ|INE177H01039,GPIL
NSE_EQ|INE470A01017,3MINDIA
NSE_EQ|INE338I01027,MOTILALOFS
NSE_EQ|INE935N01020,DIXON
NSE_EQ|INE002L01015,SJVN
NSE_EQ|INE038A01020,HINDALCO
NSE_EQ|INE031A01017,HUDCO
NSE_EQ|INE027A01015,RCF
NSE_EQ|INE242A01010,IOC
NSE_EQ|INE0DK501011,PPLPHARMA
NSE_EQ|INE0BV301023,MAPMYINDIA
NSE_EQ|INE131A01031,GMDCLTD
NSE_EQ|INE692A01016,UNIONBANK
NSE_EQ|INE477A01020,CANFINHOME
NSE_EQ|INE739E01017,CERA
NSE_EQ|INE04I401011,KPITTECH
NSE_EQ|INE061F01013,FORTIS
NSE_EQ|INE010V01017,LTTS
NSE_EQ|INE263A01024,BEL
NSE_EQ|INE120A01034,CARBORUNIV
NSE_EQ|INE020B01018,RECLTD
NSE_EQ|INE685A01028,TORNTPHARM
NSE_EQ|INE647A01010,SRF
NSE_EQ|INE491A01021,CUB
NSE_EQ|INE517F01014,GPPL
NSE_EQ|INE860A01027,HCLTECH
NSE_EQ|INE0BS701011,PREMIERENE
NSE_EQ|INE00H001014,SWIGGY
NSE_EQ|INE178A01016,CHENNPETRO
NSE_EQ|INE457A01014,MAHABANK
NSE_EQ|INE891D01026,REDINGTON
NSE_EQ|INE671H01015,SOBHA
NSE_EQ|INE278Y01022,CAMPUS
NSE_EQ|INE171A01029,FEDERALBNK
NSE_EQ|INE976G01028,RBLBANK
NSE_EQ|INE262H01021,PERSISTENT
NSE_EQ|INE084A01016,BANKINDIA
NSE_EQ|INE775A01035,MOTHERSON
NSE_EQ|INE217B01036,KAJARIACER
NSE_EQ|INE878B01027,KEI
NSE_EQ|INE599M01018,JUSTDIAL
NSE_EQ|INE325A01013,TIMKEN
NSE_EQ|INE741K01010,CREDITACC
NSE_EQ|INE018E01016,SBICARD
NSE_EQ|INE0LXG01040,OLAELEC
NSE_EQ|INE776C01039,GMRAIRPORT
NSE_EQ|INE417T01026,POLICYBZR
NSE_EQ|INE068V01023,GLAND
NSE_EQ|INE115Q01022,IKS
NSE_EQ|INE602A01031,PCBL
NSE_EQ|INE879I01012,DBREALTY
NSE_EQ|INE415G01027,RVNL
NSE_EQ|INE791I01019,BRIGADE
NSE_EQ|INE821I01022,IRB
NSE_EQ|INE323A01026,BOSCHLTD
NSE_EQ|INE320J01015,RITES
NSE_EQ|INE182A01018,PFIZER
NSE_EQ|INE548C01032,EMAMILTD
NSE_EQ|INE214T01019,LTIM
NSE_EQ|INE176B01034,HAVELLS
NSE_EQ|INE404A01024,ABSLAMC
NSE_EQ|INE545U01014,BANDHANBNK
NSE_EQ|INE152A01029,THERMAX
NSE_EQ|INE511C01022,POONAWALLA
NSE_EQ|INE150B01039,ALKYLAMINE
NSE_EQ|INE249Z01020,MAZDOCK
NSE_EQ|INE0DD101019,RAILTEL
NSE_EQ|INE087H01022,RENUKA
NSE_EQ|INE343H01029,SOLARINDS
NSE_EQ|INE732A01036,KIRLOSBROS
NSE_EQ|INE191H01014,PVRINOX
NSE_EQ|INE094J01016,UTIAMC
NSE_EQ|INE530B01024,IIFL
NSE_EQ|INE758T01015,ETERNAL
NSE_EQ|INE154A01025,ITC
NSE_EQ|INE455K01017,POLYCAB
NSE_EQ|INE406A01037,AUROPHARMA
NSE_EQ|INE387A01021,SUNDRMFAST
NSE_EQ|INE101A01026,M&M
NSE_EQ|INE208A01029,ASHOKLEY
NSE_EQ|INE303R01014,KALYANKJIL
NSE_EQ|INE148O01028,DELHIVERY
NSE_EQ|INE331A01037,RAMCOCEM
NSE_EQ|INE090A01021,ICICIBANK
NSE_EQ|INE472A01039,BLUESTARCO
NSE_EQ|INE628A01036,UPL
NSE_EQ|INE159A01016,GLAXO
NSE_EQ|INE787D01026,BALKRISIND
NSE_EQ|INE040H01021,SUZLON
NSE_EQ|INE09XN01023,AKUMS
NSE_EQ|INE018A01030,LT
NSE_EQ|INE092T01019,IDFCFIRSTB
NSE_EQ|INE700A01033,JUBLPHARMA
NSE_EQ|INE347G01014,PETRONET
NSE_EQ|INE103A01014,MRPL
NSE_EQ|INE067A01029,CGPOWER
NSE_EQ|INE438A01022,APOLLOTYRE
NSE_EQ|INE260D01016,OLECTRA
NSE_EQ|INE794A01010,NEULANDLAB
NSE_EQ|INE423A01024,ADANIENT
NSE_EQ|INE259A01022,COLPAL
NSE_EQ|INE07Y701011,POWERINDIA
NSE_EQ|INE765G01017,ICICIGI
NSE_EQ|INE257A01026,BHEL
NSE_EQ|INE774D01024,M&MFIN
NSE_EQ|INE206F01022,AIIL
NSE_EQ|INE424H01027,SUNTV
NSE_EQ|INE842C01021,MINDACORP
NSE_EQ|INE246F01010,GSPL
NSE_EQ|INE699H01024,AWL
NSE_EQ|INE647O01011,ABFRL
NSE_EQ|INE019C01026,HSCL
NSE_EQ|INE129A01019,GAIL
NSE_EQ|INE825V01034,MANYAVAR
NSE_EQ|INE731H01025,ACE
NSE_EQ|INE423Y01016,SBFC
NSE_EQ|INE481G01011,ULTRACEMCO
NSE_EQ|INE572A01036,JBCHEPHARM
NSE_EQ|INE0I7C01011,LATENTVIEW
NSE_EQ|INE233A01035,GODREJIND
NSE_EQ|INE114A01011,SAIL
NSE_EQ|INE031B01049,AJANTPHARM
NSE_EQ|INE774D08MG3,M&MFIN
NSE_EQ|INE935A01035,GLENMARK
NSE_EQ|INE003A01024,SIEMENS
NSE_EQ|INE029A01011,BPCL
NSE_EQ|INE670A01012,TATAELXSI
NSE_EQ|INE951I01027,VGUARD
NSE_EQ|INE092A01019,TATACHEM
NSE_EQ|INE200M01039,VBL
NSE_EQ|INE0DYJ01015,SYRMA
NSE_EQ|INE738I01010,ECLERX
NSE_EQ|INE00LO01017,CRAFTSMAN
NSE_EQ|INE0J5401028,HONASA
NSE_EQ|INE0Q9301021,IGIL
NSE_EQ|INE016A01026,DABUR
NSE_EQ|INE596F01018,PTCIL"""

@st.cache_data(ttl=600)
def get_cached_historical_data(access_token, key, days=300):
    try:
        session = requests.Session()
        session.headers.update({'Authorization': f'Bearer {access_token}', 'Content-Type': 'application/json'})
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        url = f"https://api.upstox.com/v2/historical-candle/{key}/day/{end_date}/{start_date}"
        response = session.get(url, timeout=10)
        data = response.json()
        if data.get('status') != 'success': return None
        candles = data.get('data', {}).get('candles', [])
        if len(candles) < 200: return None
        df = pd.DataFrame(candles, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume', 'oi'])
        df = df.drop('oi', axis=1)
        df[['open', 'high', 'low', 'close', 'volume']] = df[['open', 'high', 'low', 'close', 'volume']].astype(np.float32)
        return df.sort_values('timestamp').reset_index(drop=True)
    except: return None

def load_default_stocks():
    try:
        df = pd.read_csv(StringIO(DEFAULT_STOCKS))
        df.columns = df.columns.str.strip().str.lower()
        stock_list = [(row['instrument_key'].strip(), row['tradingsymbol'].strip()) for _, row in df.iterrows()]
        return stock_list, len(stock_list)
    except Exception as e:
        st.error(f"Error loading stocks: {str(e)}")
        return [], 0

def round_to_tick(price, tick_size=0.05):
    return round(price / tick_size) * tick_size

class WebSocketLTPManager:
    def __init__(self, access_token):
        self.access_token = access_token
        self.ws = None
        self.ltp_data = {}
        self.last_ltp = {}
        self.last_tick_time = {}
        self.is_connected = False
        self.subscribed_instruments = set()
        self.should_reconnect = True
        
    def get_ws_url(self):
        try:
            response = requests.get("https://api.upstox.com/v2/login/authorization/token", 
                                  headers={'Authorization': f'Bearer {self.access_token}'})
            return "wss://ws-api.upstox.com/v3/portfolio/stream-feed" if response.status_code == 200 else "wss://ws-api.upstox.com/v3/portfolio/stream-feed"
        except: return "wss://ws-api.upstox.com/v3/portfolio/stream-feed"
    
    def on_message(self, ws, message):
        try:
            data = json.loads(message)
            if data.get('type') == 'feed' and 'feeds' in data:
                for instrument_key, feed_data in data['feeds'].items():
                    if 'ff' in feed_data and 'marketFF' in feed_data['ff']:
                        market_data = feed_data['ff']['marketFF']
                        if 'ltpc' in market_data:
                            ltp_data = market_data['ltpc']
                            current_time = time.time()
                            if 'ltp' in ltp_data:
                                ltp = float(ltp_data['ltp'])
                                rounded_ltp = round_to_tick(ltp)
                                # Spike detection
                                spike_detected = False
                                if instrument_key in self.last_ltp:
                                    time_diff = current_time - self.last_tick_time.get(instrument_key, 0)
                                    if time_diff <= 10:
                                        price_change_pct = abs(ltp - self.last_ltp[instrument_key]) / self.last_ltp[instrument_key] * 100
                                        spike_detected = price_change_pct > 0.3
                                self.last_ltp[instrument_key] = ltp
                                self.last_tick_time[instrument_key] = current_time
                                self.ltp_data[instrument_key] = {
                                    'ltp': rounded_ltp,
                                    'volume': float(ltp_data.get('volume', 0)),
                                    'timestamp': current_time,
                                    'spike_detected': spike_detected
                                }
        except Exception as e: print(f"Message error: {e}")
    
    def on_open(self, ws):
        self.is_connected = True
        auth_msg = {"guid": "someguid", "method": "sub", "data": {"mode": "full", "instrumentKeys": list(self.subscribed_instruments)}}
        ws.send(json.dumps(auth_msg))
    
    def on_error(self, ws, error):
        print(f"WebSocket error: {error}")
        self.is_connected = False
        if self.should_reconnect: self.start_reconnect()
    
    def on_close(self, ws, close_status_code, close_msg):
        self.is_connected = False
        if self.should_reconnect: self.start_reconnect()
    
    def start_reconnect(self):
        if not hasattr(self, 'reconnect_thread') or not self.reconnect_thread.is_alive():
            self.reconnect_thread = threading.Thread(target=self.reconnect_loop, daemon=True)
            self.reconnect_thread.start()
    
    def reconnect_loop(self):
        while self.should_reconnect and not self.is_connected:
            time.sleep(5)
            try: self.connect_and_subscribe(list(self.subscribed_instruments))
            except: continue
    
    def connect_and_subscribe(self, instrument_keys):
        self.subscribed_instruments = set(instrument_keys)
        ws_url = self.get_ws_url()
        if not ws_url: return False
        websocket.enableTrace(False)
        self.ws = websocket.WebSocketApp(f"{ws_url}?access_token={self.access_token}",
                                       on_message=self.on_message, on_error=self.on_error, 
                                       on_open=self.on_open, on_close=self.on_close)
        ws_thread = threading.Thread(target=self.ws.run_forever, daemon=True)
        ws_thread.start()
        for _ in range(50):
            if self.is_connected: return True
            time.sleep(0.1)
        return False
    
    def get_market_data(self, instrument_key):
        data = self.ltp_data.get(instrument_key)
        return data if data and time.time() - data['timestamp'] < 60 else None
    
    def close(self):
        self.should_reconnect = False
        if self.ws: self.ws.close()
        self.is_connected = False

class UpstoxScreener:
    def __init__(self, access_token):
        self.access_token = access_token
        self.session = requests.Session()
        self.session.headers.update({'Authorization': f'Bearer {self.access_token}', 'Content-Type': 'application/json'})
        self.symbol_map = {}
        self.ws_manager = WebSocketLTPManager(access_token)
    
    def load_stocks_from_list(self, stock_list):
        for key, symbol in stock_list:
            self.symbol_map[str(key).strip()] = symbol.replace('NSE_EQ:', '')
        return [(key, self.symbol_map[key]) for key, _ in stock_list]
    
    def get_fallback_quote(self, key):
        try:
            url = f"https://api.upstox.com/v2/market-quote/ltp?instrument_key={key}"
            response = self.session.get(url, timeout=5)
            data = response.json()
            if data.get('status') == 'success' and key in data.get('data', {}):
                return {'ltp': float(data['data'][key]['last_price']), 'volume': 0, 'spike_detected': False}
        except: pass
        return None
    
    def get_historical_data(self, key, days=300):
        return get_cached_historical_data(self.access_token, key, days)
    
    def calculate_indicators(self, df):
        try:
            close_series = df['close']
            high_series = df['high']
            low_series = df['low']
            volume_series = df['volume']
            
            ema5_series = ta.trend.ema_indicator(close_series, window=5)
            ema13_series = ta.trend.ema_indicator(close_series, window=13)
            ema26_series = ta.trend.ema_indicator(close_series, window=26)
            sma50_series = ta.trend.sma_indicator(close_series, window=50)
            sma100_series = ta.trend.sma_indicator(close_series, window=100)
            sma200_series = ta.trend.sma_indicator(close_series, window=200)
            vol_sma50_series = ta.trend.sma_indicator(volume_series, window=50)
            rsi_series = ta.momentum.rsi(close_series, window=14)
            stoch_rsi_series = ta.momentum.stochrsi(close_series, window=14)
            macd_series = ta.trend.macd(close_series, window_fast=12, window_slow=26)
            macd_signal_series = ta.trend.macd_signal(close_series, window_fast=12, window_slow=26, window_sign=9)
            adx_series = ta.trend.adx(high_series, low_series, close_series, window=14)
            di_plus_series = ta.trend.adx_pos(high_series, low_series, close_series, window=14)
            di_minus_series = ta.trend.adx_neg(high_series, low_series, close_series, window=14)
            bb = ta.volatility.BollingerBands(close_series, window=20, window_dev=2)
            bb_upper_series = bb.bollinger_hband()
            atr_series = ta.volatility.average_true_range(high_series, low_series, close_series, window=14)
            
            return {
                'ema5': ema5_series.iloc[-1], 'ema13': ema13_series.iloc[-1], 'ema26': ema26_series.iloc[-1],
                'sma50': sma50_series.iloc[-1], 'sma100': sma100_series.iloc[-1], 'sma200': sma200_series.iloc[-1],
                'vol_sma50': vol_sma50_series.iloc[-1], 'rsi': rsi_series.iloc[-1], 'stoch_rsi': stoch_rsi_series.iloc[-1] * 100,
                'macd': macd_series.iloc[-1], 'macd_signal': macd_signal_series.iloc[-1], 'adx': adx_series.iloc[-1],
                'di_plus': di_plus_series.iloc[-1], 'di_minus': di_minus_series.iloc[-1], 'bb_upper': bb_upper_series.iloc[-1],
                'atr': atr_series.iloc[-1]
            }
        except: return None
    
    def check_conditions(self, key, symbol, min_conditions, market_data=None):
        df = self.get_historical_data(key)
        if df is None or len(df) < 200: return None
        latest = df.iloc[-1]
        prev = df.iloc[-2]
        if latest['close'] < 50: return None
        
        # Use real-time WebSocket data
        if market_data is None:
            market_data = self.ws_manager.get_market_data(key)
            if market_data is None:
                market_data = self.get_fallback_quote(key)
                if market_data is None:
                    market_data = {'ltp': latest['close'], 'volume': latest['volume'], 'spike_detected': False}
        
        current_ltp = market_data['ltp']
        current_volume = market_data['volume'] if market_data['volume'] > 0 else latest['volume']
        spike_detected = market_data.get('spike_detected', False)
        
        if current_volume < 50000: return None
        
        indicators = self.calculate_indicators(df)
        if indicators is None: return None
        
        # 15 Enhanced Conditions (including new volume spike + price acceleration)
        conditions = [
            current_ltp > indicators['ema5'] > indicators['ema13'] > indicators['ema26'],
            current_ltp > indicators['sma50'] > indicators['sma100'] > indicators['sma200'],
            indicators['rsi'] > 55,
            indicators['stoch_rsi'] > 50,
            indicators['macd'] > indicators['macd_signal'],
            indicators['adx'] > 20 and indicators['di_plus'] >= indicators['di_minus'],
            current_volume > 100000 and current_volume > indicators['vol_sma50'],
            current_ltp > latest['open'],
            current_ltp >= indicators['bb_upper'],
            current_ltp * 1.05 > df['high'].tail(200).max(),
            latest['low'] > prev['high'],
            current_ltp >= latest['high'] * 0.97,
            current_ltp >= df['high'].max() * 0.95,
            (indicators['atr'] / current_ltp) < 0.06,
            # NEW: Volume spike + price acceleration condition
            (current_ltp - prev['close'])/prev['close'] > 0.02 and current_volume > 2 * indicators['vol_sma50']
        ]
        
        conditions_met = sum(1 for c in conditions if c and not pd.isna(c))
        
        if conditions_met >= min_conditions:
            atr = indicators['atr'] if not pd.isna(indicators['atr']) else current_ltp * 0.02
            return {
                'symbol': symbol, 'cmp': current_ltp, 'volume': int(current_volume), 'atr': atr,
                'target1': current_ltp + (1.5 * atr), 'target2': current_ltp + (2.0 * atr), 
                'stoploss': current_ltp - atr, 'conditions_passed': conditions_met, 'rsi': indicators['rsi'],
                'volume_ratio': current_volume / indicators['vol_sma50'] if indicators['vol_sma50'] > 0 else 1,
                'gap_pct': ((latest['low'] / prev['high']) - 1) * 100 if prev['high'] > 0 else 0,
                'spike_detected': spike_detected
            }
        return None
    
    def setup_websocket(self, instrument_keys):
        status_text = st.empty()
        status_text.info("🔌 Connecting to WebSocket (Full Mode)...")
        success = self.ws_manager.connect_and_subscribe(instrument_keys)
        if success:
            status_text.success(f"✅ WebSocket connected! Subscribed to {len(instrument_keys)} instruments (Full Mode)")
            time.sleep(2)
            return True
        else:
            status_text.warning("⚠️ WebSocket failed, using fallback REST API")
            return False
    
    def screen_stocks(self, stock_list, min_conditions):
        results = []
        progress_bar = st.progress(0)
        status_text = st.empty()
        start_time = time.time()
        instrument_keys = [key for key, _ in stock_list]
        
        ws_connected = self.setup_websocket(instrument_keys)
        max_workers = min(16, len(stock_list))
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(self.check_conditions, key, symbol, min_conditions): (key, symbol) 
                      for key, symbol in stock_list}
            
            completed = 0
            for future in as_completed(futures):
                try:
                    result = future.result()
                    if result: results.append(result)
                    completed += 1
                    if completed % 25 == 0 or completed == len(stock_list):
                        elapsed = time.time() - start_time
                        rate = completed / elapsed if elapsed > 0 else 0
                        conn_status = "🟢 Full WS" if ws_connected else "🟡 REST API"
                        status_text.info(f"⚡ {completed}/{len(stock_list)} | Found: {len(results)} | Rate: {rate:.1f}/sec | {conn_status}")
                        progress_bar.progress(completed / len(stock_list))
                except: completed += 1
        
        total_time = time.time() - start_time
        data_source = "Full WebSocket" if ws_connected else "REST API"
        
        if results:
            conditions_passed = [r['conditions_passed'] for r in results]
            min_cond, max_cond, avg_cond = min(conditions_passed), max(conditions_passed), sum(conditions_passed) / len(conditions_passed)
            status_text.success(f"🚀 Scan completed in {total_time:.1f}s via {data_source} | Found {len(results)} signals | Conditions: {min_cond}-{max_cond} (avg: {avg_cond:.1f})")
        else:
            status_text.info(f"⏱️ Scan completed in {total_time:.1f}s via {data_source} | No signals found")
        
        progress_bar.empty()
        return sorted(results, key=lambda x: x['conditions_passed'], reverse=True)
    
    def send_telegram_notification(self, bot_token, chat_id, results):
        try:
            url = f"https://api.telegram.org/bot{bot_token}/sendMessage"
            if not results:
                message = "⚠️ No stocks met the conditions today."
            else:
                message = f"📊 *Enhanced WebSocket Screener*\n✅ {len(results)} stocks found (15 conditions)\n\n"
                for i, stock in enumerate(results, 1):
                    spike_emoji = "🔥" if stock.get('spike_detected', False) else ""
                    message += f"{i}. *{stock['symbol']}* {spike_emoji} ({stock['conditions_passed']}/15)\n"
                    message += f"   Entry: ₹{stock['cmp']:.2f} | Target: ₹{stock['target1']:.2f} | SL: ₹{stock['stoploss']:.2f}\n\n"
            
            message += f"_Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}_"
            payload = {'chat_id': chat_id, 'text': message, 'parse_mode': 'Markdown'}
            response = requests.post(url, json=payload, timeout=10)
            return response.status_code == 200, "Message sent!" if response.status_code == 200 else f"Failed: {response.text}"
        except Exception as e: return False, f"Error: {str(e)}"
    
    def cleanup(self):
        if self.ws_manager: self.ws_manager.close()

def run_screening_ui():
    with st.sidebar:
        st.header("⚙️ Configuration")
        access_token = st.text_input("Upstox Access Token", type="password")
        _, stock_count = load_default_stocks()
        st.info(f"📊 Using {stock_count} hardcoded stocks")
        min_conditions = st.slider("Minimum conditions", 1, 15, 8)
        
        # Auto-refresh configuration
        st.subheader("🔄 Auto-Refresh Settings")
        enable_auto_refresh = st.checkbox("Enable Auto-Refresh", value=False, help="Toggle automatic page refresh")
        
        if enable_auto_refresh:
            refresh_options = {
                "5 seconds": 5000,
                "10 seconds": 10000,
                "30 seconds": 30000,
                "1 minute": 60000,
                "2 minutes": 120000,
                "5 minutes": 300000
            }
            selected_interval = st.selectbox(
                "Refresh Interval", 
                options=list(refresh_options.keys()),
                index=2,  # Default to 30 seconds
                help="Choose how often to refresh the page automatically"
            )
            refresh_interval = refresh_options[selected_interval]
            st.success(f"✅ Auto-refresh: {selected_interval}")
        else:
            refresh_interval = None
            st.info("🔄 Auto-refresh disabled")
        
        st.subheader("📱 Telegram Notifications")
        bot_token = "7923075723:AAGL5-DGPSU0TLb68vOLretVwioC6vK0fJk"
        chat_id = "457632002"
        send_telegram = st.checkbox("Send Telegram notification", value=True)
        st.info("✅ Telegram configured")
        cpu_count = multiprocessing.cpu_count() or 2
        max_workers = min(16, stock_count)
        st.info(f"CPU: {cpu_count} | Workers: {max_workers}")
        screen_button = st.button("🚀 Start Enhanced Screening", type="primary")
    
    # Apply auto-refresh only if enabled
    if enable_auto_refresh and refresh_interval:
        st_autorefresh(interval=refresh_interval, key="configurable_autorefresh")
    
    return {'access_token': access_token, 'min_conditions': min_conditions, 'bot_token': bot_token, 
            'chat_id': chat_id, 'send_telegram': send_telegram, 'screen_button': screen_button,
            'enable_auto_refresh': enable_auto_refresh, 'refresh_interval': refresh_interval}

def process_results_ui(results):
    if results:
        st.success(f"Found {len(results)} stocks!")
        df_results = pd.DataFrame(results)
        df_display = pd.DataFrame({
            'Symbol': df_results['symbol'],
            'CMP': df_results['cmp'].apply(lambda x: f"₹{x:.2f}"),
            'Volume': df_results['volume'].apply(lambda x: f"{x:,}"),
            'Vol Ratio': df_results['volume_ratio'].apply(lambda x: f"{x:.1f}x"),
            'RSI': df_results['rsi'].apply(lambda x: f"{x:.1f}"),
            'Gap%': df_results['gap_pct'].apply(lambda x: f"{x:.1f}%"),
            'Target 1': df_results['target1'].apply(lambda x: f"₹{x:.2f}"),
            'Target 2': df_results['target2'].apply(lambda x: f"₹{x:.2f}"),
            'Stop Loss': df_results['stoploss'].apply(lambda x: f"₹{x:.2f}"),
            'Conditions': df_results['conditions_passed'].apply(lambda x: f"{x}/15"),
            'Alert': df_results.get('spike_detected', pd.Series([False]*len(df_results))).apply(lambda x: "🔥 SPIKE" if x else "")
        })
        
        def highlight_spikes(row):
            return ['background-color: #ffeb3b'] * len(row) if row['Alert'] == "🔥 SPIKE" else [''] * len(row)
        
        styled_df = df_display.style.apply(highlight_spikes, axis=1)
        st.dataframe(styled_df, use_container_width=True)
        
        csv_data = df_results[['symbol', 'cmp', 'volume', 'volume_ratio', 'rsi', 'gap_pct', 
                              'target1', 'target2', 'stoploss', 'conditions_passed', 'spike_detected']].copy()
        csv_data.columns = ['Symbol', 'CMP', 'Volume', 'Vol_Ratio', 'RSI', 'Gap_Pct', 
                           'Target_1', 'Target_2', 'Stop_Loss', 'Conditions', 'Spike_Alert']
        
        st.download_button("📥 Download CSV", csv_data.to_csv(index=False),
                          f"enhanced_screener_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", "text/csv")
        
        st.subheader("🏆 Top 3 Stocks")
        for i, stock in enumerate(results[:3]):
            spike_alert = "🔥 PRICE SPIKE!" if stock.get('spike_detected', False) else ""
            with st.expander(f"{i+1}. {stock['symbol']} - {stock['conditions_passed']}/15 conditions {spike_alert}"):
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("CMP (Rounded)", f"₹{stock['cmp']:.2f}")
                    st.metric("RSI", f"{stock['rsi']:.1f}")
                with col2:
                    st.metric("Volume Ratio", f"{stock['volume_ratio']:.1f}x")
                    st.metric("Gap %", f"{stock['gap_pct']:.1f}%")
                with col3:
                    st.metric("Target 1", f"₹{stock['target1']:.2f}")
                    st.metric("Stop Loss", f"₹{stock['stoploss']:.2f}")
    else:
        st.warning("No stocks found. Try lowering minimum conditions.")

def main():
    st.set_page_config(page_title="Enhanced WebSocket Screener", page_icon="🔥", layout="wide")
    st.title("🔥 Enhanced Real-Time WebSocket Screener")
    st.success("✅ Configurable Auto-Refresh | Real-Time Volume | Spike Detection | 15 Conditions | Auto-Reconnect | Full Mode")
    
    ui_config = run_screening_ui()
    
    if ui_config['access_token']:
        screener = UpstoxScreener(ui_config['access_token'])
        try:
            stock_list, stock_count = load_default_stocks()
            if not stock_list:
                st.error("Failed to load stock list")
                return
            processed_stock_list = screener.load_stocks_from_list(stock_list)
            st.success(f"Loaded {len(processed_stock_list)} stocks")
            
            with st.spinner("Enhanced WebSocket screening..."):
                results = screener.screen_stocks(processed_stock_list, ui_config['min_conditions'])
            
            if ui_config['send_telegram']:
                with st.spinner("Sending Telegram..."):
                    success, message = screener.send_telegram_notification(ui_config['bot_token'], ui_config['chat_id'], results)
                    if success: st.success(f"📱 {message}")
                    else: st.error(f"📱 {message}")
            
            process_results_ui(results)
            screener.cleanup()
            
        except Exception as e: st.error(f"Error: {str(e)}")
    elif not ui_config['access_token']: st.info("Please provide access token to start screening.")

if __name__ == "__main__": main()