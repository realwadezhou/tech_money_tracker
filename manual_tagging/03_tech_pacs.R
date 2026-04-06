# 03_tech_pacs.R
#
# Purpose: Identify PACs with a connected_org that is a tech company.
# This is derivable from cm.txt alone — no manual tagging needed for the
# initial pass, though you may want to review the edges.
#
# These are the "Google NetPAC", "Meta PAC", "Microsoft PAC" etc.
# They give modest amounts ($5K/candidate) bipartisanly to incumbents.

library(tidyverse)

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
out_dir <- file.path(project_root, "manual_tagging")
cm_path <- file.path(data_root, "fec", "interim", "2024", "cm24", "cm.txt")

# --- load ---
cm <- read_delim(cm_path, delim = "|", col_names = FALSE,
                 show_col_types = FALSE, trim_ws = TRUE) %>%
  rename(
    committee_id     = X1,  committee_name   = X2,  treasurer_name  = X3,
    street_1         = X4,  street_2         = X5,  city            = X6,
    state            = X7,  zip              = X8,  designation     = X9,
    committee_type   = X10, party            = X11, filing_frequency = X12,
    interest_group   = X13, connected_org    = X14, candidate_id    = X15
  )

# --- search connected_org for tech companies ---
# Same broad terms as the employer search
tech_org_patterns <- c(
  "GOOG", "ALPHABET", "MICROSOFT", "META PLATFORMS", "FACEBOOK",
  "APPLE", "AMAZON", "NVIDIA", "INTEL\\b", "AMD", "ADVANCED MICRO",
  "QUALCOMM", "BROADCOM", "SALESFORCE", "ORACLE", "ADOBE", "CISCO",
  "IBM", "DELL", "HEWLETT", "PALANTIR", "SNOWFLAKE",
  "TESLA", "SPACEX", "OPENAI", "ANTHROPIC",
  "UBER", "LYFT", "AIRBNB", "DOORDASH",
  "STRIPE", "COINBASE", "RIPPLE", "ROBINHOOD", "PAYPAL", "EBAY",
  "NETFLIX", "SPOTIFY", "TWITTER", "SNAP INC", "PINTEREST", "REDDIT",
  "INTUIT", "SHOPIFY", "CROWDSTRIKE", "PALO ALTO NETWORKS",
  "ZOOM VIDEO", "DROPBOX", "SERVICENOW", "WORKDAY",
  "T-MOBILE", "VERIZON", "AT&T", "COMCAST",
  "ANDREESSEN", "SEQUOIA"
)

pattern <- paste(tech_org_patterns, collapse = "|")

tech_pacs <- cm %>%
  filter(str_detect(connected_org, regex(pattern, ignore_case = TRUE))) %>%
  select(committee_id, committee_name, committee_type, designation,
         party, connected_org, candidate_id) %>%
  arrange(connected_org)

message("Found ", nrow(tech_pacs), " committees with tech-connected orgs")

# --- add columns for manual review ---
output <- tech_pacs %>%
  mutate(
    include        = NA,
    canonical_org  = NA_character_,
    sector         = NA_character_,
    notes          = NA_character_
  )

# --- write ---
out_path <- file.path(out_dir, "tech_pacs_for_review.csv")
write_csv(output, out_path)
message("Wrote ", nrow(output), " rows to ", out_path)

# --- print summary ---
tech_pacs %>%
  group_by(connected_org) %>%
  summarise(n_committees = n(), .groups = "drop") %>%
  arrange(desc(n_committees)) %>%
  print(n = 50)
