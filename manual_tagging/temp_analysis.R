# setup and read data -----------

library(tidyverse)
library(janitor)

# Walk upward until we find the project folder that contains the expected data.
find_project_root <- function(start = getwd()) {
  current <- normalizePath(start, winslash = "/", mustWork = TRUE)

  repeat {
    has_data <- dir.exists(file.path(current, "data"))
    has_lookup <- dir.exists(file.path(current, "lookup"))

    if (has_data && has_lookup) {
      return(current)
    }

    parent <- dirname(current)
    if (identical(parent, current)) {
      stop(
        "Could not find project root. Start R in the project folder or scripts folder.",
        call. = FALSE
      )
    }

    current <- parent
  }
}

root <- find_project_root()

# --- file paths ---

cm_path <- file.path(root, "data", "interim", "fec", "2026", "cm26", "cm.txt")
itcont_path <- file.path(root, "data", "interim", "fec", "2026", "indiv26", "itcont.txt")
itoth_path <- file.path(root, "data", "interim", "fec", "2026", "oth26", "itoth.txt")
lookup_path <- file.path(root, "lookup", "entity_aliases.csv")

# --- load data ---

# Committee reference table with readable committee metadata.
cm <- read_delim(
  cm_path,
  delim = "|",
  col_names = FALSE,
  show_col_types = FALSE,
  trim_ws = TRUE
) %>%
  clean_names() %>%
  rename(
    committee_id = x1,
    committee_name = x2,
    treasurer_name = x3,
    street_1 = x4,
    street_2 = x5,
    city = x6,
    state = x7,
    zip = x8,
    designation = x9,
    committee_type = x10,
    party = x11,
    filing_frequency = x12,
    interest_group_category = x13,
    connected_org_name = x14,
    candidate_id = x15
  )

# Individual contribution records with a cleaned numeric amount column.
itcont <- read_delim(
  itcont_path,
  delim = "|",
  col_names = FALSE,
  show_col_types = FALSE,
  trim_ws = TRUE
) %>%
  clean_names() %>%
  rename(
    committee_id = x1,
    amendment_indicator = x2,
    report_type = x3,
    election_type = x4,
    image_number = x5,
    transaction_type = x6,
    entity_type = x7,
    contributor_name = x8,
    city = x9,
    state = x10,
    zip = x11,
    employer = x12,
    occupation = x13,
    transaction_date = x14,
    amount = x15,
    other_id = x16,
    transaction_id = x17,
    file_number = x18,
    memo_code = x19,
    memo_text = x20,
    sub_id = x21
  ) %>%
  mutate(amount = as.numeric(amount))

# Non-individual contribution records with the same basic layout as `itcont`.
itoth <- read_delim(
  itoth_path,
  delim = "|",
  col_names = FALSE,
  show_col_types = FALSE,
  trim_ws = TRUE
) %>%
  clean_names() %>%
  rename(
    committee_id = x1,
    amendment_indicator = x2,
    report_type = x3,
    election_type = x4,
    image_number = x5,
    transaction_type = x6,
    entity_type = x7,
    contributor_name = x8,
    city = x9,
    state = x10,
    zip = x11,
    employer = x12,
    occupation = x13,
    transaction_date = x14,
    amount = x15,
    other_id = x16,
    transaction_id = x17,
    file_number = x18,
    memo_code = x19,
    memo_text = x20,
    sub_id = x21
  ) %>%
  mutate(amount = as.numeric(amount))

# --- alias lookup and tagging ---

# Local alias table used to group employer strings under a shared entity name.
lookup <- read_csv(lookup_path, show_col_types = FALSE) %>%
  filter(include_in_entity)

# Individual contributions whose employer matches one of the alias strings.
tagged <- itcont %>%
  inner_join(lookup, by = c("employer" = "alias"))

# --- entity-level summary ---

entity_summary <- tagged %>%
  group_by(entity) %>%
  summarise(
    contribution_count = n(),
    total_amount_usd = sum(amount, na.rm = TRUE),
    unique_aliases = n_distinct(employer),
    unique_contributors = n_distinct(contributor_name),
    .groups = "drop"
  ) %>%
  arrange(desc(total_amount_usd))

# --- alias-level summary ---

alias_summary <- tagged %>%
  group_by(entity, employer) %>%
  summarise(
    contribution_count = n(),
    total_amount_usd = sum(amount, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(entity, desc(contribution_count))

# --- ad hoc employer search ---
# Change the pattern and rerun to explore employer strings in itcont.

search_employer <- function(pattern, data = itcont) {
  data %>%
    filter(str_detect(employer, regex(pattern, ignore_case = TRUE))) %>%
    group_by(employer) %>%
    summarise(
      n = n(),
      total_usd = sum(amount, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    arrange(desc(total_usd))
}

# example: search_employer("META|FACEBOOK")

# Search multiple terms, combine results, and copy to clipboard for manual review.
search_and_copy <- function(terms, data = itcont) {
  results <- terms %>%
    map(~ search_employer(.x, data = data)) %>%
    bind_rows() %>%
    group_by(employer) %>%
    summarise(
      n = sum(n),
      total_usd = sum(total_usd),
      .groups = "drop"
    ) %>%
    arrange(desc(total_usd))

  write.table(results, "clipboard-16384", sep = "\t", row.names = FALSE)
  message(nrow(results), " rows copied to clipboard")

  results
}

# big tech
google_terms <- c("GOOG", "Google", "Alphabet", "Deepmind", "Youtube")
microsoft_terms <- c("Microsoft", "MSFT")
meta_terms <- c("Facebook", "Instagram", "FB", "Whatsapp", "Meta")
apple_terms <- c("Apple", "AAPL")

x_terms <- c("Twitter", "X", "XAI", "X AI")
space_x_terms <- c("SpaceX", "Space X", "Space Exploration")

open_ai_terms <- c("OpenAI", "Open AI")
anthropic_terms <- c("Anthropic")

nvidia_terms <- c("NVIDIA", "NVDA")
intel_terms <- c("Intel", "INTC")
amd_terms <- c("AMD", "Advanced Micro Devices")

search_and_copy(google_terms)


search_employer("AMD") %>% 
  print(n = 100)

#search_employer("ALPHABET")

#search_employer("ALPHABET") %>%
#  write.table("clipboard", sep = "\t", row.names = FALSE, quote = FALSE)


##### exploratory ---

itoth %>%
  group_by(employer) %>%
  summarize(sum = sum(amount)) %>%
  arrange(desc(sum)) %>%
  print(n = 50)

itcont %>%
  group_by(employer) %>%
  summarize(sum = sum(amount)) %>%
  arrange(desc(sum)) %>%
  print(n = 50)

# biggest contributors
itcont %>%
  group_by(contributor_name) %>%
  summarize(sum = sum(amount)) %>%
  arrange(desc(sum)) %>%
  print(n = 50)

# wh
itcont %>% 
  distinct(transaction_type)

itoth

itoth %>%
  summarize(sum(amount))

itcont %>%
  summarize(sum(amount))

# what committees are tagged employees giving to?
tagged %>%
  group_by(committee_id) %>%
  summarise(
    contributions = n(),
    total_usd = sum(amount, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  left_join(cm %>% select(committee_id, committee_name, committee_type, party, candidate_id), by = "committee_id") %>%
  arrange(desc(total_usd)) %>%
  print(n = 30)

# entity summary check


# some searches ----
cm %>%
  filter(grepl("leading the future", committee_name, ignore.case = TRUE)) %>% 
  write.table("clipboard", sep = "\t", row.names = FALSE, quote = FALSE)

itcont %>% 
  filter(grepl("musk, elon", contributor_name, ignore.case = TRUE)) %>% 
  print(n = 500)

itcont %>% 
  filter(grepl("musk, elon", contributor_name, ignore.case = TRUE),
         memo_code == "X") %>% 
  select(contributor_name, committee_id, transaction_type, amount, memo_code, memo_text) %>% 
  print(n = 20)


# alias breakdown
alias_summary %>%
  print(n = 30)
