import mysql.connector
import csv
import os
import openai 
import string
from us import states 

class AnalyzeData:
    def __init__(self, db_config, api_keys):
        # self.api_keys = api_keys
        self.db_config = db_config
        self.conn = self.connect_to_db()
        self.cursor = self.conn.cursor(dictionary=True)

        self.gpt_client = openai.Client(api_key=api_keys['Openai API'])  

        self.urlcontent_charlimit = 1500
       
    def __del__(self):
        if self.conn and self.conn.is_connected():
            self.conn.close()

    def connect_to_db(self):
        try:
            conn = mysql.connector.connect(
                host=self.db_config.host,
                user=self.db_config.user,
                password=self.db_config.password,
                database=self.db_config.db_name
            )
            return conn

        except mysql.connector.Error as err:
            print(f"Error in connect_to_db(): {err}")
            return None


    def create_exact_table_from_csv(self, csv_file):
        """Creates a MySQL table that exactly matches the CSV structure (NO extra id column)."""
        try:
            # Extract table name from CSV filename (remove extension)
            table_name = os.path.splitext(os.path.basename(csv_file))[0]

            with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.reader(file)
                headers = next(reader)  # Read headers from CSV

                # Keep column names as they are, just wrap them in backticks
                formatted_headers = [f"`{col.strip()}`" for col in headers]

                # Create table structure
                column_definitions = ", ".join([f"{col} VARCHAR(255)" for col in formatted_headers])
                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    {column_definitions}
                );
                """
                self.cursor.execute(create_table_query)
                self.conn.commit()
                print(f"Table `{table_name}` created successfully in `{self.db_config.db_name}`.")

                # Prepare the INSERT query
                placeholders = ", ".join(["%s"] * len(headers))
                insert_query = f"INSERT INTO `{table_name}` ({', '.join(formatted_headers)}) VALUES ({placeholders})"

                # Debugging: Print the query to check for errors
                print(f"DEBUG - Insert Query: {insert_query}")

                # Insert all rows from the CSV into MySQL
                for row in reader:
                    self.cursor.execute(insert_query, row)

                self.conn.commit()
                print(f"Data from `{csv_file}` copied successfully into `{table_name}`.")

        except FileNotFoundError:
            print(f"Error: The file {csv_file} was not found.")
        except mysql.connector.Error as err:
            print(f"MySQL Error in create_table_from_csv(): {err}")
        except Exception as e:
            print(f"Error in create_table_from_csv(): {e}")




    def create_table_from_csv(self, csv_file):
        """Creates a table in MySQL and copies the entire data from the CSV file into it."""
        try:
            # Extract table name from CSV filename (without extension)
            table_name = os.path.splitext(os.path.basename(csv_file))[0]

            with open(csv_file, mode='r', newline='', encoding='utf-8') as file:
                reader = csv.reader(file)
                headers = next(reader)  # Read headers from CSV
                
                # Preserve spaces in column names but wrap them in backticks
                formatted_headers = [f"`{col.strip()}`" for col in headers]

                # If CSV already has an `id` column, use it; otherwise, add an auto-increment ID
                if "`id`" in formatted_headers:
                    column_definitions = ", ".join([f"{col} VARCHAR(255)" for col in formatted_headers])
                else:
                    column_definitions = "`id` INT AUTO_INCREMENT PRIMARY KEY, " + ", ".join([f"{col} VARCHAR(255)" for col in formatted_headers])

                create_table_query = f"""
                CREATE TABLE IF NOT EXISTS `{table_name}` (
                    {column_definitions}
                );
                """
                self.cursor.execute(create_table_query)
                self.conn.commit()
                print(f"Table `{table_name}` created successfully in `{self.db_config.db_name}`.")

                # Insert all rows from the CSV
                placeholders = ", ".join(["%s"] * len(headers))
                insert_query = f"INSERT INTO `{table_name}` ({', '.join(formatted_headers)}) VALUES ({placeholders})"

                for row in reader:
                    self.cursor.execute(insert_query, row)

                self.conn.commit()
                print(f"Data from `{csv_file}` copied successfully into `{table_name}`.")

        except FileNotFoundError:
            print(f"Error: The file {csv_file} was not found.")
        except mysql.connector.Error as err:
            print(f"MySQL Error in create_table_from_csv(): {err}")
        except Exception as e:
            print(f"Error in create_table_from_csv(): {e}")




    def CheckNames(self, table_name):
        try:
            # Fetch all first names and last names from the specified table
            query = f"SELECT `First Name`, `Last Name` FROM `{table_name}`"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            # List to store invalid names
            invalid_first_names = []  
            invalid_first_names = []   

            for row in rows:
                # Remove extra spaces
                first_name_str = row["First Name"].strip()  
                last_name_str = row["Last Name"].strip()    

                # Validation rules for first name
                is_valid_first = (
                    bool(first_name_str)                    # Must not be empty
                    and len(first_name_str) > 1             # Ensure length is more than 1 (except "A" or "I")
                    and first_name_str.isalpha()            # Must contain only alphabetic characters
                    and " " not in first_name_str           # Must be a single word (no spaces allowed)
                    and first_name_str[0].isupper()         # Ensure the first letter is capitalized
                )

                is_valid_last = (
                    bool(last_name_str)                    
                    and len(last_name_str) > 1              
                    and last_name_str.isalpha()             
                    and " " not in last_name_str            
                    and last_name_str[0].isupper()          
                )

                if not is_valid_first:
                    invalid_first_names.append(first_name_str)    # Store only invalid first names

                if not is_valid_last:
                    invalid_first_names.append(last_name_str)      # Store only invalid last names

            return invalid_first_names, invalid_first_names  # Return two lists

        except mysql.connector.Error as err:
            print(f"MySQL Error in CheckNames(): {err}")
            return [], []  # Return empty lists if there's an error

        except Exception as e:
            print(f"Error in CheckNames(): {e}")
            return [], []  # Return empty lists if there's an error



    def Row_Position_of_Invalid_Names(self, table_name, invalid_first_names, invalid_last_names):
        try:
            invalid_first_entries = []
            invalid_last_entries = []

            # Process invalid first names
            if invalid_first_names:
                formatted_first_names = ", ".join(f"'{name}'" for name in invalid_first_names)
                query_first = f"SELECT `id`, `First Name` FROM `{table_name}` WHERE `First Name` IN ({formatted_first_names})"
                
                self.cursor.execute(query_first)
                rows_first = self.cursor.fetchall()
                
                # Store tuples (incremented id, first name)
                invalid_first_entries = [(row["id"] + 1, row["First Name"]) for row in rows_first]

            # Process invalid last names
            if invalid_last_names:
                formatted_last_names = ", ".join(f"'{name}'" for name in invalid_last_names)
                query_last = f"SELECT `id`, `Last Name` FROM `{table_name}` WHERE `Last Name` IN ({formatted_last_names})"
                
                self.cursor.execute(query_last)
                rows_last = self.cursor.fetchall()
                
                # Store tuples (incremented id, last name)
                invalid_last_entries = [(row["id"] + 1, row["Last Name"]) for row in rows_last]

            return invalid_first_entries, invalid_last_entries  # Return two separate lists

        except mysql.connector.Error as err:
            print(f"MySQL Error in Row_Position_of_Invalid_Names(): {err}")
            return [], []  # Return empty lists in case of an error

        except Exception as e:
            print(f"Error in Row_Position_of_Invalid_Names(): {e}")
            return [], []  # Return empty lists in case of an error



    def row_position_of_invalid_states(self, table_name, invalid_mailing_states):
        try:
            invalid_state_entries = []

            if invalid_mailing_states:
                formatted_states = ", ".join(f"'{state}'" for state in invalid_mailing_states)
                query_last = f"SELECT `id`, `Mailing State` FROM `{table_name}` WHERE `Mailing State` IN ({formatted_states})"
                
                self.cursor.execute(query_last)
                rows = self.cursor.fetchall()
                
                # Store tuples (incremented id, last name)
                invalid_state_entries = [(row["id"] + 1, row["Mailing State"]) for row in rows]

            return invalid_state_entries

        except mysql.connector.Error as err:
            print(f"MySQL Error in row_position_of_invalid_states(): {err}")
            return [], []  # Return empty lists in case of an error

        except Exception as e:
            print(f"Error in row_position_of_invalid_states(): {e}")
            return [], []  # Return empty lists in case of an error    




    def Row_Position_of_Invalid_Cities_from_Valid_States(self, table_name, Invalid_Cities):
        try:
            invalid_city_entries = []

            if Invalid_Cities:
                # Format city-state pairs for SQL query
                conditions = " OR ".join(
                    f"(`Mailing City` = '{city}' AND `Mailing State` = '{state}')" 
                    for city, state in Invalid_Cities
                )

                # Updated SQL query to fetch IDs where both city and state match
                query = f"SELECT `id`, `Mailing City`, `Mailing State` FROM `{table_name}` WHERE {conditions}"

                self.cursor.execute(query)
                rows = self.cursor.fetchall()
                
                # Store tuples (incremented id, city, state)
                invalid_city_entries = [(row["id"] + 1, row["Mailing City"], row["Mailing State"]) for row in rows]

            return invalid_city_entries  # Returns list of (row_position, city, state) tuples

        except mysql.connector.Error as err:
            print(f"MySQL Error in Row_Position_of_Invalid_Cities_from_Valid_States(): {err}")
            return []

        except Exception as e:
            print(f"Error in Row_Position_of_Invalid_Cities_from_Valid_States(): {e}")
            return []


    def Row_Position_of_Invalid_Zip_from_Valid_Cities(self, table_name, Valid_Cities):
        try:
            invalid_zip_entries = []

            if Valid_Cities:
                # Extract only city and state from the input list (ignoring ZIP codes)
                formatted_conditions = " OR ".join(
                    f"(`Mailing City` = '{city}' AND `Mailing State` = '{state}')" 
                    for zip, city, state in Valid_Cities  # ✅ Corrected unpacking
                )

                # Updated SQL query to fetch IDs and ZIP codes where both city and state match
                query = f"SELECT `id`, `Mailing City`, `Mailing State`, `Mailing Zip` FROM `{table_name}` WHERE {formatted_conditions}"

                self.cursor.execute(query)
                rows = self.cursor.fetchall()
                
                # Store tuples (incremented id, zip, city, state)
                invalid_zip_entries = [(row["id"] + 1, row["Mailing Zip"], row["Mailing City"], row["Mailing State"]) for row in rows]

            return invalid_zip_entries  # Returns list of (row_position, zip, city, state) tuples

        except mysql.connector.Error as err:
            print(f"MySQL Error in Row_Position_of_Invalid_Zip_from_Valid_Cities(): {err}")
            return []

        except Exception as e:
            print(f"Error in Row_Position_of_Invalid_Zip_from_Valid_Cities(): {e}")
            return []


    def CheckStates(self, table_name):
        try:
            # Fetch all mailing states from the specified table
            query = f"SELECT `Mailing State` FROM `{table_name}`"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            invalid_mailing_states = []  # List to store invalid states

            # Get a set of valid USPS state abbreviations
            valid_state_abbreviations = {s.abbr for s in states.STATES}

            for row in rows:
                state_str = row["Mailing State"].strip()  # Remove extra spaces

                is_valid_state = state_str in valid_state_abbreviations

                if not is_valid_state:
                    invalid_mailing_states.append(state_str)  # Store only invalid states

            return invalid_mailing_states  # Return invalid states list

        except mysql.connector.Error as err:
            print(f"MySQL Error in CheckStates(): {err}")
            return []  # Return empty list if there's an error

        except Exception as e:
            print(f"Error in CheckStates(): {e}")
            return []  # Return empty list if there's an error




    def Dict_of_City_with_Valid_State(self, table_name, invalid_states):
        try:
            # Convert list to a string of comma-separated values for SQL query
            invalid_states_str = ",".join(f"'{state}'" for state in invalid_states)  # Ensure states are properly quoted

            query = f"SELECT `Mailing City`, `Mailing State` FROM `{table_name}` WHERE `Mailing State` NOT IN ({invalid_states_str})"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()
            
            dict_statetocity = {}  # Dictionary where the key is the state and the value is a list of cities

            for row in rows:
                city = row["Mailing City"].strip()
                state = row["Mailing State"].strip()

                if state not in dict_statetocity:
                    dict_statetocity[state] = []  # Initialize list if state not already in dict

                dict_statetocity[state].append(city)  # Append city to state's list

            return dict_statetocity  

        except mysql.connector.Error as err:
            print(f"MySQL Error in Dict_of_City_with_Valid_State(): {err}")
            return {}  # Return an empty dictionary if there's an error

        except Exception as e:
            print(f"Error in Dict_of_City_with_Valid_State(): {e}")
            return {}  # Return an empty dictionary if there's an error



    def validate_cities_with_valid_states_with_openai(self, city_state_dict):
        invalid_city_state_pairs = []
        valid_city_state_pairs = [] 

        for state, cities in city_state_dict.items():
            for city in cities:
                prompt = (
                    f"Verify if the city or unincorporated community '{city}' exists in the state '{state}' in the United States."
                    " Return the response in the following format:\n"
                    "- YES: If the city exists in the specified state.\n"
                    "- NO: If the city does not exist in the given state or is misspelled."
                    #"- NO (Correction: Correct City/State if applicable): If the city does not exist in the given state or is misspelled."
                )

                try:
                    response = self.gpt_client.chat.completions.create(  
                        model="gpt-4", 
                        messages=[{"role": "system", "content": "You are a geographic validator."},
                                {"role": "user", "content": prompt}],  
                        max_tokens=50,
                        temperature=0
                    )

                    ai_response = response.choices[0].message.content.strip()

                    if ai_response.startswith("NO"):
                        # print(f"Invalid: {city}, {state} -> {ai_response}")
                        # invalid_city_state_pairs.append((city, state, ai_response))  # Store invalid pairs with corrections
                        invalid_city_state_pairs.append((city, state))  # Store invalid pairs with corrections

                    elif ai_response.startswith("YES"):
                        valid_city_state_pairs.append((city, state))  # Store invalid pairs with corrections
                
                except Exception as e:
                    print(f"Error querying OpenAI: {e}")

        return invalid_city_state_pairs, valid_city_state_pairs 




    def List_of_tuples_with_zip_and_corresponding_valid_cities(self, table_name, valid_city_state_tuples):
        try:
            # Convert list of tuples to a properly formatted SQL condition
            valid_conditions = " OR ".join(f"(`Mailing City` = '{city}' AND `Mailing State` = '{state}')" for city, state in valid_city_state_tuples)

            # Construct the SQL query
            query = f"SELECT `Mailing Zip`, `Mailing City`, `Mailing State` FROM `{table_name}` WHERE {valid_conditions}"
            self.cursor.execute(query)
            rows = self.cursor.fetchall()

            # Convert the result into a list of tuples (Mailing Zip, Mailing City)
            result_list = [(row["Mailing Zip"], row["Mailing City"], row["Mailing State"]) for row in rows]

            return result_list  

        except mysql.connector.Error as err:
            print(f"MySQL Error in List_of_tuples_with_zip_and_corresponding_valid_cities(): {err}")
            return []  # Return an empty list if there's an error

        except Exception as e:
            print(f"Error in List_of_tuples_with_zip_and_corresponding_valid_cities(): {e}")
            return []  




    def validate_zip_with_valid_cities_with_openai(self, list_of_tuples_with_zip_from_valid_cities):
        invalid_zip_city_pair = []
        valid_zip_city_pair = []

        for zip_code, city, state in list_of_tuples_with_zip_from_valid_cities:
            prompt = (
                f"Verify if the ZIP code '{zip_code}' correctly corresponds to the city '{city}' and state '{state}' in the United States. "
                "Return the response in the following format:\n"
                "- YES: If the ZIP code is valid for the specified city.\n"
                "- NO: If the ZIP code does not match the city or is incorrect."
            )

            try:
                response = self.gpt_client.chat.completions.create(  
                    model="gpt-4", 
                    messages=[{"role": "system", "content": "You are a ZIP code validator for US cities."},
                            {"role": "user", "content": prompt}],  
                    max_tokens=50,
                    temperature=0
                )

                ai_response = response.choices[0].message.content.strip()

                if ai_response.startswith("NO"):
                    invalid_zip_city_pair.append((zip_code, city, state))  # Store invalid pairs
                elif ai_response.startswith("YES"):
                    valid_zip_city_pair.append((zip_code, city, state))  # Store valid pairs

            except Exception as e:
                print(f"Error querying OpenAI: {e}")

        return invalid_zip_city_pair, valid_zip_city_pair






    def get_headers_mapping(self, table_name, cursor):
        try:
            # Query to get column names from the table
            query = f"SHOW COLUMNS FROM `{table_name}`"
            cursor.execute(query)
            columns = cursor.fetchall()

            # Generate dictionary mapping column names to letters (A, B, C...)
            column_mapping = {col["Field"]: letter for col, letter in zip(columns, string.ascii_uppercase)}

            return column_mapping

        except Exception as e:
            print(f"Error in get_headers_mapping(): {e}")
            return {}






    def get_missing_table(self, table_name):
        """Retrieves data from a table with missing values count."""
        try:
            query = f"""
            SELECT 
                id,
                client_id,
                donation_date,
                donation_source,
                donation_count,
                avg_donation,
                total_donation,
                -- Count the number of NULL, empty, or whitespace-only values in the row
                (CASE WHEN donation_date IS NULL OR TRIM(donation_date) = '' THEN 1 ELSE 0 END +
                CASE WHEN donation_source IS NULL OR TRIM(donation_source) = '' THEN 1 ELSE 0 END +
                CASE WHEN donation_count IS NULL OR TRIM(donation_count) = '' THEN 1 ELSE 0 END +
                CASE WHEN avg_donation IS NULL OR TRIM(avg_donation) = '' THEN 1 ELSE 0 END +
                CASE WHEN total_donation IS NULL OR TRIM(total_donation) = '' THEN 1 ELSE 0 END) 
                AS missing_value_count
            FROM `{table_name}`
            WHERE 
                -- Filter rows where at least 1 column is NULL, empty, or whitespace-only
                (CASE WHEN donation_date IS NULL OR TRIM(donation_date) = '' THEN 1 ELSE 0 END +
                CASE WHEN donation_source IS NULL OR TRIM(donation_source) = '' THEN 1 ELSE 0 END +
                CASE WHEN donation_count IS NULL OR TRIM(donation_count) = '' THEN 1 ELSE 0 END +
                CASE WHEN avg_donation IS NULL OR TRIM(avg_donation) = '' THEN 1 ELSE 0 END +
                CASE WHEN total_donation IS NULL OR TRIM(total_donation) = '' THEN 1 ELSE 0 END) >= 1;
            """

            self.cursor.execute(query)
            results = self.cursor.fetchall()  # Fetch all matching rows as a list of tuples

            return results

        except mysql.connector.Error as err:
            print(f"MySQL Error in get_table(): {err}")
            return None
        except Exception as e:
            print(f"Error in get_table(): {e}")
            return None


    def find_missing_values(self, data):
        """
        Returns a list of tuples containing the 'id' and a list of column names where values are missing.
        """
        missing_pairs = []

        for row in data:
            missing_keys = [key for key, value in row.items() if value == "" or value is None]

            if missing_keys:  # Only add rows with missing values
                missing_pairs.append((row['id'], missing_keys))

        return missing_pairs
