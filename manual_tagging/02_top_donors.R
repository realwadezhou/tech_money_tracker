# 02_top_donors.R
#
# Purpose: Generate a list of the top individual donors by total giving,
# for manual review. Outputs two CSVs:
#
# 1. top_donors_summary.csv — one row per contributor_name, ranked by net
#    total giving. You scan this to decide who's a "tech person."
#
# 2. top_donors_variants.csv — expanded: one row per (contributor_name,
#    employer, occupation) triple. You fill in consolidated_name to group
#    name variants (e.g. "MUSK, ELON" and "MUSK, ELON R" -> "Elon Musk").
#
# Data source: itcont (2024). Transaction type rules applied so amounts are
# meaningful.

library(tidyverse)

# --- paths ---
data_root   <- "C:/Users/Wade/Documents/projects/spending_tracker/data"
out_dir     <- "C:/Users/Wade/Documents/projects/tech_money/manual_tagging"
itcont_path <- file.path(data_root, "interim", "fec", "2024", "indiv24", "itcont.txt")

# --- load itcont ---
message("Loading itcont...")
itcont <- read_delim(
  itcont_path,
  delim = "|", col_names = FALSE,
  show_col_types = FALSE, trim_ws = TRUE
) %>%
  rename(
    committee_id     = X1,  amendment_ind    = X2,  report_type     = X3,
    election_type    = X4,  image_number     = X5,  transaction_type = X6,
    entity_type      = X7,  contributor_name = X8,  city            = X9,
    state            = X10, zip              = X11, employer        = X12,
    occupation       = X13, transaction_date = X14, amount          = X15,
    other_id         = X16, transaction_id   = X17, file_number     = X18,
    memo_code        = X19, memo_text        = X20, sub_id          = X21
  ) %>%
  mutate(amount = as.numeric(amount))

message("Loaded ", nrow(itcont), " rows")

# --- apply transaction type rules ---
include_types <- c("10", "15", "15E", "15C", "11",
                   "30", "31", "32", "30E", "31E", "32E",
                   "30T", "31T", "32T", "42Y", "41Y")
refund_types  <- c("22Y", "21Y")

# Memo X handling is type-specific. Only exclude memo X where we've verified
# it means routing/double-count:
#   - 15E memo X: earmark memo line on conduit filing (the 24T is the real row). EXCLUDE.
#   - 24T: already not in include_types.
#   - Type 10 memo X: real money — in-kind contributions, trust/partnership
#     attributions, earmarks. $180M including Musk, Andreessen, Horowitz. INCLUDE.
#   - Other types: memo X is uncommon and small; include to avoid losing real money.
memo_x_exclude_types <- c("15E")

contributions <- itcont %>%
  filter(transaction_type %in% c(include_types, refund_types)) %>%
  filter(entity_type %in% c("IND", "CAN", "")) %>%
  filter(memo_code != "X" | !transaction_type %in% memo_x_exclude_types)

# Net amount: refund types get flipped
contributions <- contributions %>%
  mutate(
    net_amount = case_when(
      transaction_type %in% refund_types ~ -amount,
      TRUE ~ amount
    )
  )

message("After filtering: ", nrow(contributions), " contribution rows")

# --- build the variant-level table ---
# One row per (contributor_name, employer, occupation) triple
variants <- contributions %>%
  group_by(contributor_name, employer, occupation) %>%
  summarise(
    n_contributions = n(),
    net_usd         = sum(net_amount, na.rm = TRUE),
    states          = paste(unique(na.omit(state[state != ""])), collapse = "; "),
    n_committees    = n_distinct(committee_id),
    .groups = "drop"
  )

# --- build the summary table ---
# One row per contributor_name, for ranking purposes
summary_tbl <- contributions %>%
  group_by(contributor_name) %>%
  summarise(
    net_total_usd   = sum(net_amount, na.rm = TRUE),
    n_contributions = n(),
    n_variants      = n_distinct(paste(employer, occupation, sep = "|||")),
    employers       = paste(unique(na.omit(employer[employer != ""])), collapse = "; "),
    occupations     = paste(unique(na.omit(occupation[occupation != ""])), collapse = "; "),
    states          = paste(unique(na.omit(state[state != ""])), collapse = "; "),
    n_committees    = n_distinct(committee_id),
    .groups = "drop"
  ) %>%
  arrange(desc(net_total_usd))

message("Unique contributor_names: ", nrow(summary_tbl))

# --- take top N ---
n_to_review <- 2000
top_names <- summary_tbl %>% slice_head(n = n_to_review)

message("Top donor #1: ", top_names$contributor_name[1],
        " ($", format(top_names$net_total_usd[1], big.mark = ","), ")")
message("Top donor #", n_to_review, ": ", top_names$contributor_name[n_to_review],
        " ($", format(top_names$net_total_usd[n_to_review], big.mark = ","), ")")

# --- write summary CSV ---
summary_out <- top_names %>%
  mutate(
    is_tech          = NA,
    consolidated_name = NA_character_,
    tech_role        = NA_character_,
    tech_company     = NA_character_,
    notes            = NA_character_
  )

write_csv(summary_out, file.path(out_dir, "top_donors_summary.csv"))
message("Wrote top_donors_summary.csv (", nrow(summary_out), " rows)")

# --- write variants CSV ---
# Only for the top N donors — expand to show every name/employer/occupation combo
top_variants <- variants %>%
  filter(contributor_name %in% top_names$contributor_name) %>%
  arrange(contributor_name, desc(net_usd)) %>%
  mutate(
    consolidated_name = NA_character_,  # you fill this in
    is_tech           = NA,
    tech_company      = NA_character_,
    notes             = NA_character_
  )

write_csv(top_variants, file.path(out_dir, "top_donors_variants.csv"))
message("Wrote top_donors_variants.csv (", nrow(top_variants), " rows)")

# --- also flag potential name collisions ---
# Names like "SMITH, JOHN" could be multiple people. Flag names that appear
# in 3+ states as possibly needing disambiguation.
multi_state <- summary_tbl %>%
  filter(contributor_name %in% top_names$contributor_name) %>%
  mutate(n_states = str_count(states, ";") + 1) %>%
  filter(n_states >= 3) %>%
  select(contributor_name, net_total_usd, n_states, states, employers) %>%
  arrange(desc(n_states))

write_csv(multi_state, file.path(out_dir, "top_donors_multi_state_warning.csv"))
message("Wrote multi-state warning file (", nrow(multi_state),
        " donors appearing in 3+ states — possible name collisions)")
