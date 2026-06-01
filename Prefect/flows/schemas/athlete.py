from google.cloud.bigquery import SchemaField

ATHLETE_SCHEMA = [
    SchemaField("id",             "INTEGER",   "NULLABLE"),
    SchemaField("username",       "STRING",    "NULLABLE"),
    SchemaField("resource_state", "INTEGER",   "NULLABLE"),
    SchemaField("firstname",      "STRING",    "NULLABLE"),
    SchemaField("lastname",       "STRING",    "NULLABLE"),
    SchemaField("bio",            "STRING",    "NULLABLE"),
    SchemaField("city",           "STRING",    "NULLABLE"),
    SchemaField("state",          "STRING",    "NULLABLE"),
    SchemaField("country",        "STRING",    "NULLABLE"),
    SchemaField("sex",            "STRING",    "NULLABLE"),
    SchemaField("premium",        "BOOLEAN",   "NULLABLE"),
    SchemaField("summit",         "BOOLEAN",   "NULLABLE"),
    SchemaField("created_at",     "TIMESTAMP", "NULLABLE"),
    SchemaField("updated_at",     "TIMESTAMP", "NULLABLE"),
    SchemaField("badge_type_id",  "INTEGER",   "NULLABLE"),
    SchemaField("profile_medium", "STRING",    "NULLABLE"),
    SchemaField("profile",        "STRING",    "NULLABLE"),
    SchemaField("friend",         "STRING",    "NULLABLE"),
    SchemaField("follower",       "STRING",    "NULLABLE"),
]
