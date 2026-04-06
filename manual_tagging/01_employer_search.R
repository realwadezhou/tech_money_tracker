# 01_employer_search.R
#
# Purpose: Cast a wide net across itcont employer strings to find all plausible
# tech companies. Outputs a CSV for manual review — you mark each row as
# include/exclude and optionally assign a canonical entity name.
#
# Data source: itcont only (that's where the employer field lives).
# We use 2024 data for this tagging exercise, then apply the same tags to 2026.

library(tidyverse)
library(janitor)

# --- paths ---
args <- commandArgs(trailingOnly = FALSE)
file_arg <- grep("^--file=", args, value = TRUE)
if (length(file_arg) > 0) {
  script_dir <- dirname(normalizePath(sub("^--file=", "", file_arg[1])))
} else {
  script_dir <- getwd()
}

project_root <- normalizePath(file.path(script_dir, ".."))
data_root <- file.path(project_root, "data")
out_dir <- file.path(project_root, "data", "reference", "tech_employers")

itcont_path <- file.path(data_root, "fec", "interim", "2024", "indiv24", "itcont.txt")

# --- load itcont ---
message("Loading itcont (this takes a minute)...")
itcont <- read_delim(
  itcont_path,
  delim = "|",
  col_names = FALSE,
  show_col_types = FALSE,
  trim_ws = TRUE
) %>%
  rename(
    committee_id     = X1,
    amendment_ind    = X2,
    report_type      = X3,
    election_type    = X4,
    image_number     = X5,
    transaction_type = X6,
    entity_type      = X7,
    contributor_name = X8,
    city             = X9,
    state            = X10,
    zip              = X11,
    employer         = X12,
    occupation       = X13,
    transaction_date = X14,
    amount           = X15,
    other_id         = X16,
    transaction_id   = X17,
    file_number      = X18,
    memo_code        = X19,
    memo_text        = X20,
    sub_id           = X21
  ) %>%
  mutate(amount = as.numeric(amount))

message("Loaded ", nrow(itcont), " rows")

# --- helper: search employer strings ---
search_employer <- function(pattern, data = itcont) {
  data %>%
    filter(str_detect(employer, regex(pattern, ignore_case = TRUE))) %>%
    group_by(employer) %>%
    summarise(
      n_contributions = n(),
      total_usd       = sum(amount, na.rm = TRUE),
      n_donors        = n_distinct(contributor_name),
      .groups = "drop"
    ) %>%
    arrange(desc(total_usd))
}

# --- define search terms by company/sector ---
# These are intentionally broad to catch variants. False positives are expected
# and will be removed during manual review.

tech_searches <- list(
  google = c("\\bGOOG\\b", "\\bGOOGL\\b", "GOOGLE", "GOOGLE LLC", "ALPHABET", "GOOGLE CLOUD", "DEEPMIND", "GOOGLE DEEPMIND", "GOOGLE BRAIN", "YOUTUBE", "WAYMO", "VERILY"),
  microsoft = c("\\bMSFT\\b", "MICROSOFT", "MICROSOFT CORP(?:ORATION)?", "AZURE", "LINKEDIN", "GITHUB", "MICROSOFT RESEARCH"),
  meta = c("META PLATFORMS", "FACEBOOK", "INSTAGRAM", "WHATSAPP", "META AI", "REALITY LABS"),
  apple = c("\\bAAPL\\b", "\\bAPPLE\\b", "APPLE INC", "APPLE COMPUTER"),
  amazon = c("\\bAMZN\\b", "AMAZON", "AMAZON\\.COM", "AMAZON WEB SERVICES", "\\bAWS\\b", "TWITCH", "ZOOX", "KUIPER"),
  
  openai = c("OPEN[[:space:]]?AI"),
  anthropic = c("ANTHROPIC", "ANTHROPIC PBC"),
  xai = c("\\bXAI\\b", "\\bX\\.AI\\b", "X AI", "GROK"),
  mistral = c("MISTRAL AI"),
  cohere = c("COHERE AI", "\\bCOHERE\\b(?![[:space:]]+HEALTH)"),
  perplexity = c("PERPLEXITY", "PERPLEXITY AI"),
  scale = c("SCALE AI", "SCALEAI"),
  huggingface = c("HUGGING[[:space:]-]?FACE"),
  stability = c("STABILITY AI", "STABLE DIFFUSION"),
  runway = c("RUNWAYML", "RUNWAY AI"),
  ssi = c("SAFE SUPERINTELLIGENCE", "SSI INC"),
  together = c("TOGETHER AI", "TOGETHER COMPUTER"),
  character = c("CHARACTER\\.AI", "CHARACTER AI"),
  midjourney = c("MIDJOURNEY"),
  elevenlabs = c("ELEVEN[[:space:]]?LABS"),
  langchain = c("LANGCHAIN", "LANGSMITH"),
  glean = c("GLEAN TECHNOLOGIES", "\\bGLEAN\\b"),
  anduril = c("ANDURIL", "ANDURIL INDUSTRIES"),
  coreweave = c("\\bCRWV\\b", "COREWEAVE"),
  wandb = c("WEIGHTS[[:space:]]*&[[:space:]]*BIASES", "WEIGHTS AND BIASES", "\\bWANDB\\b"),
  c3ai = c("C3\\.AI", "C3 AI", "C3AI"),
  soundhound = c("SOUNDHOUND AI", "SOUNDHOUND"),
  tempus = c("TEMPUS AI"),
  uipath = c("UIPATH"),
  groq = c("\\bGROQ\\b"),
  sambanova = c("SAMBANOVA", "SAMBA NOVA"),
  cursor = c("ANYSPHERE", "\\bCURSOR\\b"),
  
  tesla = c("\\bTSLA\\b", "TESLA"),
  spacex = c("SPACEX", "SPACE X", "SPACE EXPLORATION TECH"),
  boring = c("BORING COMPANY"),
  neuralink = c("NEURALINK"),
  
  nvidia = c("\\bNVDA\\b", "NVIDIA", "NVIDIA CORP(?:ORATION)?"),
  intel = c("\\bINTC\\b", "\\bINTEL\\b", "INTEL CORP(?:ORATION)?"),
  amd = c("\\bAMD\\b", "ADVANCED MICRO DEVICES"),
  qualcomm = c("\\bQCOM\\b", "QUALCOMM"),
  broadcom = c("\\bAVGO\\b", "BROADCOM"),
  tsmc = c("\\bTSM\\b", "\\bTSMC\\b", "TAIWAN SEMICONDUCTOR"),
  arm = c("ARM HOLDINGS"),
  micron = c("MICRON", "MICRON TECHNOLOGY"),
  marvell = c("\\bMRVL\\b", "MARVELL"),
  
  salesforce = c("SALESFORCE", "SLACK", "TABLEAU", "MULESOFT"),
  oracle = c("\\bORCL\\b", "\\bORACLE\\b", "NETSUITE", "CERNER", "ORACLE HEALTH"),
  sap = c("SAP SE", "SAP AMERICA"),
  adobe = c("\\bADBE\\b", "\\bADOBE\\b"),
  vmware = c("VMWARE"),
  snowflake = c("SNOWFLAKE", "SNOWFLAKE COMPUTING"),
  palantir = c("\\bPLTR\\b", "PALANTIR"),
  databricks = c("DATABRICKS", "MOSAICML", "MOSAIC AI"),
  servicenow = c("SERVICENOW", "SERVICE[[:space:]]?NOW", "MOVEWORKS"),
  workday = c("\\bWDAY\\b", "WORKDAY"),
  atlassian = c("ATLASSIAN", "JIRA", "CONFLUENCE", "TRELLO", "LOOM"),
  hubspot = c("HUBSPOT"),
  datadog = c("\\bDDOG\\b", "DATADOG"),
  cloudflare = c("CLOUDFLARE"),
  mongodb = c("\\bMDB\\b", "MONGODB"),
  okta = c("OKTA"),
  gitlab = c("\\bGTLB\\b", "GITLAB"),
  box = c("BOX,? INC\\.?", "BOX\\.COM"),
  
  twitter = c("TWITTER", "TWITTER INC", "X CORP", "X\\.COM"),
  snap = c("SNAP INC", "SNAP INC\\.", "\\bSNAP\\b(?!PLE|[[:space:]-]?ON|[[:space:]-]?CHAT)"),
  snapchat = c("SNAPCHAT"),
  pinterest = c("PINTEREST"),
  reddit = c("\\bRDDT\\b", "\\bREDDIT\\b"),
  tiktok = c("TIKTOK", "TIK TOK", "BYTEDANCE"),
  spotify = c("SPOTIFY"),
  discord = c("\\bDISCORD\\b"),
  
  uber = c("\\bUBER\\b"),
  lyft = c("\\bLYFT\\b", "LYFT INC"),
  doordash = c("DOORDASH", "DOOR[[:space:]]?DASH"),
  instacart = c("INSTACART", "MAPLEBEAR"),
  airbnb = c("\\bABNB\\b", "AIRBNB"),
  
  stripe = c("\\bSTRIPE\\b"),
  block_inc = c("BLOCK,? INC\\.?", "BLOCK INC", "SQUARE", "CASH APP", "AFTERPAY", "TIDAL", "BITKEY", "PROTO"),
  coinbase = c("COINBASE"),
  ripple = c("RIPPLE", "RIPPLE LABS"),
  robinhood = c("ROBINHOOD"),
  plaid = c("\\bPLAID\\b"),
  a16z = c("ANDREESSEN HOROWITZ", "A16Z"),
  sequoia = c("SEQUOIA CAPITAL", "SEQUOIA CAP"),
  
  kleiner = c("KLEINER PERKINS", "KLEINER"),
  khosla = c("KHOSLA VENTURES", "KHOSLA"),
  greylock = c("GREYLOCK"),
  benchmark = c("BENCHMARK CAPITAL"),
  accel = c("\\bACCEL\\b(?! ENTER)"),
  
  dell = c("\\bDELL\\b(?! MONTE| ICIOUS)", "DELL TECHNOLOGIES"),
  hp = c("\\bHPQ\\b", "HP INC", "HEWLETT[[:space:]-]?PACKARD"),
  hpe = c("\\bHPE\\b", "HEWLETT[[:space:]-]?PACKARD ENTERPRISE"),
  cisco = c("\\bCSCO\\b", "\\bCISCO\\b"),
  ibm = c("\\bIBM\\b"),
  
  att = c("AT[[:space:]]*&[[:space:]]*T", "\\bATT\\b", "AT AND T"),
  verizon = c("VERIZON"),
  tmobile = c("\\bTMUS\\b", "T[[:space:]-]?MOBILE"),
  comcast = c("\\bCMCSA\\b", "COMCAST"),
  
  crowdstrike = c("\\bCRWD\\b", "CROWDSTRIKE", "CROWD STRIKE"),
  palo_alto = c("\\bPANW\\b", "PALO ALTO NETWORKS"),
  fortinet = c("\\bFTNT\\b", "FORTINET"),
  sentinelone = c("SENTINELONE", "SENTINEL ONE"),
  zscaler = c("\\bZS\\b", "ZSCALER"),
  
  netflix = c("\\bNFLX\\b", "NETFLIX"),
  zoom = c("ZOOM COMMUNICATIONS", "ZOOM VIDEO", "\\bZOOM\\b(?! INFO)"),
  dropbox = c("\\bDBX\\b", "DROPBOX"),
  figma = c("\\bFIGMA\\b", "FIGMA INC"),
  canva = c("\\bCANVA\\b"),
  shopify = c("SHOPIFY"),
  intuit = c("\\bINTU\\b", "\\bINTUIT\\b", "MAILCHIMP", "CREDIT KARMA", "TURBOTAX", "QUICKBOOKS"),
  paypal = c("\\bPYPL\\b", "PAYPAL", "VENMO"),
  ebay = c("\\bEBAY\\b")
)

# these are for exact matches; they would likely raise false positives
# if part of broader searches
exact_ticker_only <- list(
  meta = c("META", "FB"),
  arm = c("ARM"),
  sap = c("SAP"),
  salesforce = c("CRM"),
  servicenow = c("NOW"),
  snowflake = c("SNOW"),
  atlassian = c("TEAM"),
  hubspot = c("HUBS"),
  cloudflare = c("NET"),
  box = c("BOX"),
  c3ai = c("AI"),
  soundhound = c("SOUN"),
  tempus = c("TEM"),
  uipath = c("PATH"),
  coinbase = c("COIN"),
  robinhood = c("HOOD"),
  doordash = c("DASH"),
  instacart = c("CART"),
  pinterest = c("PINS"),
  spotify = c("SPOT"),
  shopify = c("SHOP"),
  att = c("T"),
  verizon = c("VZ"),
  figma = c("FIG"),
  sentinelone = c("S"),
  vmware = c("VMW"),
  twitter = c("TWTR"),
  block_inc = c("SQ", "XYZ")
)

# --- run all regex searches ---
message("Running regex employer searches...")

all_results <- imap_dfr(tech_searches, function(terms, company) {
  pattern <- paste(terms, collapse = "|")
  matches <- search_employer(pattern)
  if (nrow(matches) > 0) {
    matches %>% mutate(search_company = company, match_type = "regex")
  } else {
    tibble()
  }
})

message("Regex matches: ", nrow(all_results), " employer strings")

# --- run exact-match searches ---
# These terms are too short or ambiguous for regex — we only count them
# if the entire employer field is exactly that string (after trimming).
message("Running exact-match employer searches...")

exact_results <- imap_dfr(exact_ticker_only, function(terms, company) {
  matches <- itcont %>%
    mutate(employer_trimmed = str_trim(toupper(employer))) %>%
    filter(employer_trimmed %in% toupper(terms)) %>%
    group_by(employer) %>%
    summarise(
      n_contributions = n(),
      total_usd       = sum(amount, na.rm = TRUE),
      n_donors        = n_distinct(contributor_name),
      .groups = "drop"
    ) %>%
    arrange(desc(total_usd))

  if (nrow(matches) > 0) {
    matches %>% mutate(search_company = company, match_type = "exact")
  } else {
    tibble()
  }
})

message("Exact matches: ", nrow(exact_results), " employer strings")

# --- combine and deduplicate ---
combined <- bind_rows(all_results, exact_results)

deduped <- combined %>%
  group_by(employer) %>%
  summarise(
    n_contributions  = first(n_contributions),
    total_usd        = first(total_usd),
    n_donors         = first(n_donors),
    matched_searches = paste(unique(search_company), collapse = "; "),
    match_types      = paste(unique(match_type), collapse = "; "),
    .groups = "drop"
  ) %>%
  arrange(desc(total_usd))

message("Found ", nrow(deduped), " unique employer strings total (regex + exact)")

# --- flag likely false positives for attention during review ---
# These are patterns we know will be noisy
noisy_patterns <- c(
  "SQUARE",      # construction, town squares, etc
  "PROTO",       # prototyping labs, biotech
  "TIDAL",       # could be non-tech
  "SLACK",       # uncommon but possible
  "\\bZS\\b",    # ZS Associates (consulting) vs Zscaler
  "\\bCURSOR\\b" # could be non-tech
)
noisy_regex <- paste(noisy_patterns, collapse = "|")

deduped <- deduped %>%
  mutate(
    flagged_noisy = str_detect(employer, regex(noisy_regex, ignore_case = TRUE))
  )

n_flagged <- sum(deduped$flagged_noisy, na.rm = TRUE)
message("Flagged ", n_flagged, " employer strings as potentially noisy — check these first")

# --- add columns for manual review ---
output <- deduped %>%
  mutate(
    include        = NA,            # TRUE/FALSE — you fill this in
    canonical_name = NA_character_,  # e.g. "Google" — you fill this in
    sector         = NA_character_,  # e.g. "big_tech", "ai", "semiconductor", "fintech"
    notes          = NA_character_   # anything you want to note
  )

# --- write CSV ---
out_path <- file.path(out_dir, "employer_matches_for_review.csv")
write_csv(output, out_path)
message("Wrote ", nrow(output), " rows to ", out_path)

# --- quick summary ---
message("\nSummary by search term:")
combined %>%
  group_by(search_company, match_type) %>%
  summarise(
    employer_strings = n(),
    total_usd        = sum(total_usd, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(total_usd)) %>%
  print(n = 150)
