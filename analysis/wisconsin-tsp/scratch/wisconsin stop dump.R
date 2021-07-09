library(sf)
library(tidyverse)
library(mapview)

pattern_stops <-
  read_csv(
    "stop_index_out.csv" 
  ) %>%
  st_as_sf(., 
           coords = c("stop_lon", "stop_lat"),
           crs = 4326L, #WGS84
           agr = "constant"
           )

patterns <-
  pattern_stops %>%
  count(route, direction, pattern, pattern_destination)

show_stops <-
  pattern_stops %>%
  filter(route == "30N" & direction == "WEST")


mapview::mapview(show_stops, zcol = "stop_id")      

write_sf(
  pattern_stops,
  "pattern_stops.geojson"
)


# Reimport the stops on the corridor --------------------------------------

consin_stops <-
  tibble::tribble(
    ~`OBJECTID`, ~`shapes`,     ~X1, ~route, ~direction, ~pattern,           ~pattern_destination, ~stop_id, ~order_, ~stop_sort_order,                      ~geo_description, ~stop_sequence,
    41L,  "Point Z",     40L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   20776L,     66L,              58L,   "WISCONSIN AVE NW + GARFIELD ST NW",            59L,
    42L,  "Point Z",     41L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   17836L,     68L,              59L, "WISCONSIN AVE NW + CATHEDRAL AVE NW",            60L,
    43L,  "Point Z",     42L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   20027L,     69L,              60L,    "WISCONSIN AVE NW + WOODLEY RD NW",            61L,
    44L,  "Point Z",     43L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   33729L,     70L,              61L,     "WISCONSIN AVE NW + MACOMB ST NW",            62L,
    45L,  "Point Z",     44L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   27415L,     71L,              62L,     "WISCONSIN AVE NW + NEWARK ST NW",            63L,
    46L,  "Point Z",     45L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   20483L,     72L,              63L,     "WISCONSIN AVE NW + PORTER ST NW",            64L,
    47L,  "Point Z",     46L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   17279L,     73L,              64L,     "WISCONSIN AVE NW + RODMAN ST NW",            65L,
    48L,  "Point Z",     47L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   41072L,     74L,              65L,      "WISCONSIN AVE NW + UPTON ST NW",            66L,
    49L,  "Point Z",     48L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   30808L,     75L,              66L,     "WISCONSIN AVE NW + VEAZEY ST NW",            67L,
    50L,  "Point Z",     49L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   18291L,     76L,              67L,       "WISCONSIN AVE NW + TENLEY CIR",            68L,
    51L,  "Point Z",     50L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   25622L,     78L,              68L,  "WISCONSIN AVE NW + ALBEMARLE ST NW",            69L,
    52L,  "Point Z",     51L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",      44L,     79L,              69L, "WISCONSIN AVE NW + BRANDYWINE ST NW",            70L,
    53L,  "Point Z",     52L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   26700L,     80L,              70L, "WISCONSIN AVE NW + CHESAPEAKE ST NW",            71L,
    54L,  "Point Z",     53L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   24637L,     81L,              71L,  "WISCONSIN AVE NW + FESSENDEN ST NW",            72L,
    55L,  "Point Z",     54L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   23320L,     82L,              72L,   "WISCONSIN AVE NW + HARRISON ST NW",            73L,
    56L,  "Point Z",     55L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",      49L,     83L,              73L,    "WISCONSIN AVE NW + JENIFER ST NW",            74L,
    57L,  "Point Z",     56L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",      50L,     84L,              74L,      "WISCONSIN AVE NW + WESTERN AVE",            75L,
    58L,  "Point Z",     57L,  "30N",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   32089L,     86L,              75L,  "FRIENDSHIP HEIGHTS STA + BUS BAY C",            76L,
    69L,  "Point Z",    126L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   32089L,      2L,               1L,  "FRIENDSHIP HEIGHTS STA + BUS BAY C",             2L,
    70L,  "Point Z",    127L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",      58L,      3L,               2L,    "WISCONSIN AVE NW + JENIFER ST NW",             3L,
    71L,  "Point Z",    128L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   25718L,      4L,               3L,   "WISCONSIN AVE NW + HARRISON ST NW",             4L,
    72L,  "Point Z",    129L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   23697L,      5L,               4L,  "WISCONSIN AVE NW + FESSENDEN ST NW",             5L,
    73L,  "Point Z",    130L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   19762L,      6L,               5L, "WISCONSIN AVE NW + CHESAPEAKE ST NW",             6L,
    74L,  "Point Z",    131L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",      59L,      7L,               6L,      "WISCONSIN AVE NW + RIVER RD NW",             7L,
    75L,  "Point Z",    132L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   23789L,      9L,               7L,  "WISCONSIN AVE NW + ALBEMARLE ST NW",             8L,
    76L,  "Point Z",    133L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   34865L,     10L,               8L, "WISCONSIN AVE NW + TENLEY CIRCLE NW",             9L,
    77L,  "Point Z",    134L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",      62L,     11L,               9L,   "WISCONSIN AVE NW + VAN NESS ST NW",            10L,
    78L,  "Point Z",    135L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",    4556L,     12L,              10L,          "WISCONSIN AVE + UPTON ST X",            11L,
    79L,  "Point Z",    136L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   22916L,     13L,              11L,     "WISCONSIN AVE NW + RODMAN ST NW",            12L,
    80L,  "Point Z",    137L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   18041L,     14L,              12L,     "WISCONSIN AVE NW + PORTER ST NW",            13L,
    81L,  "Point Z",    138L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   26349L,     15L,              13L,           "WISCONSIN AVE + NEWARK ST",            14L,
    82L,  "Point Z",    139L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   21381L,     16L,              14L,    "WISCONSIN AVE NW + WOODLEY RD NW",            15L,
    83L,  "Point Z",    140L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   25495L,     18L,              15L, "WISCONSIN AVE NW + CATHEDRAL AVE NW",            16L,
    84L,  "Point Z",    141L,  "30N",     "EAST",       2L,    "EAST to NAYLOR RD STATION",   25214L,     19L,              16L,   "WISCONSIN AVE NW + GARFIELD ST NW",            17L,
    181L,  "Point Z", 193177L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   20776L,     67L,              59L,   "WISCONSIN AVE NW + GARFIELD ST NW",            60L,
    182L,  "Point Z", 193178L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   17836L,     69L,              60L, "WISCONSIN AVE NW + CATHEDRAL AVE NW",            61L,
    183L,  "Point Z", 193179L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   20027L,     70L,              61L,    "WISCONSIN AVE NW + WOODLEY RD NW",            62L,
    184L,  "Point Z", 193180L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   33729L,     71L,              62L,     "WISCONSIN AVE NW + MACOMB ST NW",            63L,
    185L,  "Point Z", 193181L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   27415L,     72L,              63L,     "WISCONSIN AVE NW + NEWARK ST NW",            64L,
    186L,  "Point Z", 193182L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   20483L,     73L,              64L,     "WISCONSIN AVE NW + PORTER ST NW",            65L,
    187L,  "Point Z", 193183L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   17279L,     74L,              65L,     "WISCONSIN AVE NW + RODMAN ST NW",            66L,
    188L,  "Point Z", 193184L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   41072L,     75L,              66L,      "WISCONSIN AVE NW + UPTON ST NW",            67L,
    189L,  "Point Z", 193185L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   30808L,     76L,              67L,     "WISCONSIN AVE NW + VEAZEY ST NW",            68L,
    190L,  "Point Z", 193186L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   18291L,     77L,              68L,       "WISCONSIN AVE NW + TENLEY CIR",            69L,
    191L,  "Point Z", 193187L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   25622L,     79L,              69L,  "WISCONSIN AVE NW + ALBEMARLE ST NW",            70L,
    192L,  "Point Z", 193188L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",      44L,     80L,              70L, "WISCONSIN AVE NW + BRANDYWINE ST NW",            71L,
    193L,  "Point Z", 193189L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   26700L,     81L,              71L, "WISCONSIN AVE NW + CHESAPEAKE ST NW",            72L,
    194L,  "Point Z", 193190L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   24637L,     82L,              72L,  "WISCONSIN AVE NW + FESSENDEN ST NW",            73L,
    195L,  "Point Z", 193191L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   23320L,     83L,              73L,   "WISCONSIN AVE NW + HARRISON ST NW",            74L,
    196L,  "Point Z", 193192L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",      49L,     84L,              74L,    "WISCONSIN AVE NW + JENIFER ST NW",            75L,
    197L,  "Point Z", 193193L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",      50L,     85L,              75L,      "WISCONSIN AVE NW + WESTERN AVE",            76L,
    198L,  "Point Z", 193194L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",      58L,      3L,               2L,    "WISCONSIN AVE NW + JENIFER ST NW",             3L,
    199L,  "Point Z", 193195L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   25718L,      4L,               3L,   "WISCONSIN AVE NW + HARRISON ST NW",             4L,
    200L,  "Point Z", 193196L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   23697L,      5L,               4L,  "WISCONSIN AVE NW + FESSENDEN ST NW",             5L,
    201L,  "Point Z", 193197L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   19762L,      6L,               5L, "WISCONSIN AVE NW + CHESAPEAKE ST NW",             6L,
    202L,  "Point Z", 193198L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",      59L,      7L,               6L,      "WISCONSIN AVE NW + RIVER RD NW",             7L,
    203L,  "Point Z", 193199L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   23789L,      9L,               7L,  "WISCONSIN AVE NW + ALBEMARLE ST NW",             8L,
    204L,  "Point Z", 193200L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   34865L,     10L,               8L, "WISCONSIN AVE NW + TENLEY CIRCLE NW",             9L,
    205L,  "Point Z", 193201L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",      62L,     11L,               9L,   "WISCONSIN AVE NW + VAN NESS ST NW",            10L,
    206L,  "Point Z", 193202L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",    4556L,     12L,              10L,          "WISCONSIN AVE + UPTON ST X",            11L,
    207L,  "Point Z", 193203L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   22916L,     13L,              11L,     "WISCONSIN AVE NW + RODMAN ST NW",            12L,
    208L,  "Point Z", 193204L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   18041L,     14L,              12L,     "WISCONSIN AVE NW + PORTER ST NW",            13L,
    209L,  "Point Z", 193205L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   26349L,     15L,              13L,           "WISCONSIN AVE + NEWARK ST",            14L,
    210L,  "Point Z", 193206L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   21381L,     16L,              14L,    "WISCONSIN AVE NW + WOODLEY RD NW",            15L,
    211L,  "Point Z", 193207L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   25495L,     18L,              15L, "WISCONSIN AVE NW + CATHEDRAL AVE NW",            16L,
    212L,  "Point Z", 193208L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   25214L,     19L,              16L,   "WISCONSIN AVE NW + GARFIELD ST NW",            17L,
    284L,  "Point Z", 193449L,  "30S",     "WEST",       1L,   "WEST to FRIENDSHIP HEIGHTS",   32089L,     87L,              76L,  "FRIENDSHIP HEIGHTS STA + BUS BAY C",            77L,
    285L,  "Point Z", 193586L,  "30S",     "EAST",       2L, "EAST to SOUTHERN AVE STATION",   32089L,      2L,               1L,  "FRIENDSHIP HEIGHTS STA + BUS BAY C",             2L,
    317L,  "Point Z", 392672L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   20776L,     21L,              19L,   "WISCONSIN AVE NW + GARFIELD ST NW",            20L,
    318L,  "Point Z", 392673L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   17836L,     23L,              20L, "WISCONSIN AVE NW + CATHEDRAL AVE NW",            21L,
    319L,  "Point Z", 392674L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   20027L,     24L,              21L,    "WISCONSIN AVE NW + WOODLEY RD NW",            22L,
    320L,  "Point Z", 392675L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   33729L,     25L,              22L,     "WISCONSIN AVE NW + MACOMB ST NW",            23L,
    321L,  "Point Z", 392676L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   27415L,     26L,              23L,     "WISCONSIN AVE NW + NEWARK ST NW",            24L,
    322L,  "Point Z", 392677L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   20483L,     27L,              24L,     "WISCONSIN AVE NW + PORTER ST NW",            25L,
    323L,  "Point Z", 392678L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   17279L,     28L,              25L,     "WISCONSIN AVE NW + RODMAN ST NW",            26L,
    324L,  "Point Z", 392679L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   41072L,     29L,              26L,      "WISCONSIN AVE NW + UPTON ST NW",            27L,
    325L,  "Point Z", 392680L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   30808L,     30L,              27L,     "WISCONSIN AVE NW + VEAZEY ST NW",            28L,
    326L,  "Point Z", 392681L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   18291L,     31L,              28L,       "WISCONSIN AVE NW + TENLEY CIR",            29L,
    327L,  "Point Z", 392682L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   25622L,     33L,              29L,  "WISCONSIN AVE NW + ALBEMARLE ST NW",            30L,
    328L,  "Point Z", 392683L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",      44L,     34L,              30L, "WISCONSIN AVE NW + BRANDYWINE ST NW",            31L,
    329L,  "Point Z", 392684L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   26700L,     35L,              31L, "WISCONSIN AVE NW + CHESAPEAKE ST NW",            32L,
    330L,  "Point Z", 392685L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   24637L,     36L,              32L,  "WISCONSIN AVE NW + FESSENDEN ST NW",            33L,
    331L,  "Point Z", 392686L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   23320L,     37L,              33L,   "WISCONSIN AVE NW + HARRISON ST NW",            34L,
    332L,  "Point Z", 392687L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",      49L,     38L,              34L,    "WISCONSIN AVE NW + JENIFER ST NW",            35L,
    333L,  "Point Z", 392688L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",      50L,     39L,              35L,      "WISCONSIN AVE NW + WESTERN AVE",            36L,
    334L,  "Point Z", 392689L,   "31",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   32089L,     41L,              36L,  "FRIENDSHIP HEIGHTS STA + BUS BAY C",            37L,
    335L,  "Point Z", 392725L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   32089L,      2L,               1L,  "FRIENDSHIP HEIGHTS STA + BUS BAY C",             2L,
    336L,  "Point Z", 392726L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",      58L,      3L,               2L,    "WISCONSIN AVE NW + JENIFER ST NW",             3L,
    337L,  "Point Z", 392727L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   25718L,      4L,               3L,   "WISCONSIN AVE NW + HARRISON ST NW",             4L,
    338L,  "Point Z", 392728L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   23697L,      5L,               4L,  "WISCONSIN AVE NW + FESSENDEN ST NW",             5L,
    339L,  "Point Z", 392729L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   19762L,      6L,               5L, "WISCONSIN AVE NW + CHESAPEAKE ST NW",             6L,
    340L,  "Point Z", 392730L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",      59L,      7L,               6L,      "WISCONSIN AVE NW + RIVER RD NW",             7L,
    341L,  "Point Z", 392731L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   23789L,      9L,               7L,  "WISCONSIN AVE NW + ALBEMARLE ST NW",             8L,
    342L,  "Point Z", 392732L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   34865L,     10L,               8L, "WISCONSIN AVE NW + TENLEY CIRCLE NW",             9L,
    343L,  "Point Z", 392733L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",      62L,     11L,               9L,   "WISCONSIN AVE NW + VAN NESS ST NW",            10L,
    344L,  "Point Z", 392734L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",    4556L,     12L,              10L,          "WISCONSIN AVE + UPTON ST X",            11L,
    345L,  "Point Z", 392735L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   22916L,     13L,              11L,     "WISCONSIN AVE NW + RODMAN ST NW",            12L,
    346L,  "Point Z", 392736L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   18041L,     14L,              12L,     "WISCONSIN AVE NW + PORTER ST NW",            13L,
    347L,  "Point Z", 392737L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   26349L,     15L,              13L,           "WISCONSIN AVE + NEWARK ST",            14L,
    348L,  "Point Z", 392738L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   21381L,     16L,              14L,    "WISCONSIN AVE NW + WOODLEY RD NW",            15L,
    349L,  "Point Z", 392739L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   25495L,     18L,              15L, "WISCONSIN AVE NW + CATHEDRAL AVE NW",            16L,
    350L,  "Point Z", 392740L,   "31",    "SOUTH",       1L,        "SOUTH to POTOMAC PARK",   25214L,     19L,              16L,   "WISCONSIN AVE NW + GARFIELD ST NW",            17L,
    370L,  "Point Z", 580826L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   32089L,      2L,               1L,  "FRIENDSHIP HEIGHTS STA + BUS BAY C",             2L,
    371L,  "Point Z", 580827L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",      58L,      3L,               2L,    "WISCONSIN AVE NW + JENIFER ST NW",             3L,
    372L,  "Point Z", 580828L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   25718L,      4L,               3L,   "WISCONSIN AVE NW + HARRISON ST NW",             4L,
    373L,  "Point Z", 580829L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   23697L,      5L,               4L,  "WISCONSIN AVE NW + FESSENDEN ST NW",             5L,
    374L,  "Point Z", 580830L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   19762L,      6L,               5L, "WISCONSIN AVE NW + CHESAPEAKE ST NW",             6L,
    375L,  "Point Z", 580831L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",      59L,      7L,               6L,      "WISCONSIN AVE NW + RIVER RD NW",             7L,
    376L,  "Point Z", 580832L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   23789L,      9L,               7L,  "WISCONSIN AVE NW + ALBEMARLE ST NW",             8L,
    377L,  "Point Z", 580833L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   34865L,     10L,               8L, "WISCONSIN AVE NW + TENLEY CIRCLE NW",             9L,
    378L,  "Point Z", 580834L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",      62L,     11L,               9L,   "WISCONSIN AVE NW + VAN NESS ST NW",            10L,
    379L,  "Point Z", 580835L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",    4556L,     12L,              10L,          "WISCONSIN AVE + UPTON ST X",            11L,
    380L,  "Point Z", 580836L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   22916L,     13L,              11L,     "WISCONSIN AVE NW + RODMAN ST NW",            12L,
    381L,  "Point Z", 580837L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   18041L,     14L,              12L,     "WISCONSIN AVE NW + PORTER ST NW",            13L,
    382L,  "Point Z", 580838L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   26349L,     15L,              13L,           "WISCONSIN AVE + NEWARK ST",            14L,
    383L,  "Point Z", 580839L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   21381L,     16L,              14L,    "WISCONSIN AVE NW + WOODLEY RD NW",            15L,
    384L,  "Point Z", 580840L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   25495L,     18L,              15L, "WISCONSIN AVE NW + CATHEDRAL AVE NW",            16L,
    385L,  "Point Z", 580841L,   "33",    "SOUTH",       1L,    "SOUTH to FEDERAL TRIANGLE",   25214L,     19L,              16L,   "WISCONSIN AVE NW + GARFIELD ST NW",            17L,
    434L,  "Point Z", 580890L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   20776L,     28L,              25L,   "WISCONSIN AVE NW + GARFIELD ST NW",            26L,
    435L,  "Point Z", 580891L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   17836L,     30L,              26L, "WISCONSIN AVE NW + CATHEDRAL AVE NW",            27L,
    436L,  "Point Z", 580892L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   20027L,     31L,              27L,    "WISCONSIN AVE NW + WOODLEY RD NW",            28L,
    437L,  "Point Z", 580893L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   33729L,     32L,              28L,     "WISCONSIN AVE NW + MACOMB ST NW",            29L,
    438L,  "Point Z", 580894L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   27415L,     33L,              29L,     "WISCONSIN AVE NW + NEWARK ST NW",            30L,
    439L,  "Point Z", 580895L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   20483L,     34L,              30L,     "WISCONSIN AVE NW + PORTER ST NW",            31L,
    440L,  "Point Z", 580896L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   17279L,     35L,              31L,     "WISCONSIN AVE NW + RODMAN ST NW",            32L,
    441L,  "Point Z", 580897L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   41072L,     36L,              32L,      "WISCONSIN AVE NW + UPTON ST NW",            33L,
    442L,  "Point Z", 580898L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   30808L,     37L,              33L,     "WISCONSIN AVE NW + VEAZEY ST NW",            34L,
    443L,  "Point Z", 580899L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   18291L,     38L,              34L,       "WISCONSIN AVE NW + TENLEY CIR",            35L,
    444L,  "Point Z", 580900L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   25622L,     40L,              35L,  "WISCONSIN AVE NW + ALBEMARLE ST NW",            36L,
    445L,  "Point Z", 580901L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",      44L,     41L,              36L, "WISCONSIN AVE NW + BRANDYWINE ST NW",            37L,
    446L,  "Point Z", 580902L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   26700L,     42L,              37L, "WISCONSIN AVE NW + CHESAPEAKE ST NW",            38L,
    447L,  "Point Z", 580903L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   24637L,     43L,              38L,  "WISCONSIN AVE NW + FESSENDEN ST NW",            39L,
    448L,  "Point Z", 580904L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   23320L,     44L,              39L,   "WISCONSIN AVE NW + HARRISON ST NW",            40L,
    449L,  "Point Z", 580905L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",      49L,     45L,              40L,    "WISCONSIN AVE NW + JENIFER ST NW",            41L,
    450L,  "Point Z", 580906L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",      50L,     46L,              41L,      "WISCONSIN AVE NW + WESTERN AVE",            42L,
    451L,  "Point Z", 580987L,   "33",    "NORTH",       2L,  "NORTH to FRIENDSHIP HEIGHTS",   32089L,     48L,              42L,  "FRIENDSHIP HEIGHTS STA + BUS BAY C",            43L
  )

consin_stops_out <-
  consin_stops %>%
  select(
    -OBJECTID,
    -shapes,
    -X1
  )

consin_stops_out %>% write_csv("wisconsin_corridor_stops.csv")



