class DatabaseConfig:
    def __init__(self, host, user, password, db_name):
        self.host = host
        self.user = user
        self.password = password
        self.db_name = db_name



db_config = DatabaseConfig(
    host = 'localhost',
    user = 'root',
    password = '',
    db_name = ''
)



# api_keys = {
#     'Openai API' : ''
# }

# db_config = DatabaseConfig(
#     host = 'localhost',
#     user = 'root',
#     password = '',
#     db_name = ''
# )
