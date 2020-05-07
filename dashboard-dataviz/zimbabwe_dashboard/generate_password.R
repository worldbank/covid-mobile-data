# Generate Username and Passwords

# Define Password --------------------------------------------------------------
PASSWORD <- readline(prompt="Enter password: ")

# Define Usernames and Passwords -----------------------------------------------
password_df <- bind_rows(
  data.frame(username = "t",   hashed_password = hashpw(PASSWORD, salt=gensalt(10)), stringsAsFactors = F),
  data.frame(username = "t",   hashed_password = hashpw(PASSWORD, salt=gensalt(10)), stringsAsFactors = F),
  data.frame(username = "t",   hashed_password = hashpw(PASSWORD, salt=gensalt(10)), stringsAsFactors = F),
  data.frame(username = "t",   hashed_password = hashpw(PASSWORD, salt=gensalt(10)), stringsAsFactors = F)
)

# Export -----------------------------------------------------------------------
saveRDS(password_df, file.path(GITHUB_PATH, "dashboard-dataviz", "zimbabwe_dashboard", "passwords.Rds"))




