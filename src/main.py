from config import db_config, api_keys
from analyze import AnalyzeData


def Create_Tables(analyzer_obj):
    analyzer_obj.create_table_from_csv("DA1/Test_Data_1.csv")
    analyzer_obj.create_exact_table_from_csv("DA1/Test_Data_2.csv")


def Invalid_Names(analyzer_obj):
    invalid_first_names, invalid_last_names = analyzer_obj.CheckNames("Test_Data_1")
    return invalid_first_names, invalid_last_names
    # print("Invalid First Names:", invalid_first_names)
    # print("Invalid Last Names:", invalid_last_names)


def Invalid_States(analyzer_obj):
    invalid_mailing_states = analyzer_obj.CheckStates("Test_Data_1")
    return invalid_mailing_states
    # print(invalid_mailing_states)


def Invalid_Cities_from_Valid_States(analyzer_obj):
    invalid_mailing_states = analyzer_obj.CheckStates("Test_Data_1")    
    Cities_with_Valid_State = analyzer_obj.Dict_of_City_with_Valid_State("Test_Data_1", invalid_mailing_states)
    Invalid_City, Valid_City = analyzer_obj.validate_cities_with_valid_states_with_openai(Cities_with_Valid_State)

    return Invalid_City
    # print("Invalid Cities:", Invalid_City)
    # print()
    # print("Valid Cities:", Valid_City)


def Invalid_Zip_from_Valid_Cities(analyzer_obj):
    invalid_mailing_states = analyzer_obj.CheckStates("Test_Data_1")    
    Cities_with_Valid_State = analyzer_obj.Dict_of_City_with_Valid_State("Test_Data_1", invalid_mailing_states)
    Invalid_City, Valid_City = analyzer_obj.validate_cities_with_valid_states_with_openai(Cities_with_Valid_State)
    list_of_tuples_with_zip_from_valid_cities = analyzer_obj.List_of_tuples_with_zip_and_corresponding_valid_cities("Test_Data_1", Valid_City)
    # print(list_of_tuples_with_zip_from_valid_cities)
    Invalid_Zip, Valid_Zip = analyzer_obj.validate_zip_with_valid_cities_with_openai(list_of_tuples_with_zip_from_valid_cities)

    return Invalid_Zip

    # print("Invalid Zip:", Invalid_Zip)
    # print()
    # print("Valid Zip:", Valid_Zip)


"""FUNCTIONS FOR OUTPUTING CELL POSITION:"""

def Position_of_Invalid_Names(analyzer_obj): 
    invalid_first_names, invalid_last_names = Invalid_Names(analyzer_obj)
    row_position_and_invalid_first, row_position_and_invalid_last = analyzer_obj.Row_Position_of_Invalid_Names("Test_Data_1", invalid_first_names, invalid_last_names)

    print("\nInvalid Last Names:")
    for row in row_position_and_invalid_last:
        print(f"Cell D{row[0]} - {row[1]}")

    print("\nInvalid First Names:")
    for row in row_position_and_invalid_first:
        print(f"Cell A{row[0]} - {row[1]}")


def Position_of_Invalid_States(analyzer_obj): 
    invalid_mailing_states = Invalid_States(analyzer_obj)
    row_position_of_invalid_states = analyzer_obj.row_position_of_invalid_states("Test_Data_1", invalid_mailing_states)

    print("\nInvalid Mailing States:")
    for row in row_position_of_invalid_states:
        print(f"Cell H{row[0]} - {row[1]}")


def Position_of_Invalid_Cities_from_Valid_States(analyzer_obj):
    Invalid_Cities = Invalid_Cities_from_Valid_States(analyzer_obj)
    row_position_of_invalid_city = analyzer_obj.Row_Position_of_Invalid_Cities_from_Valid_States("Test_Data_1", Invalid_Cities)
    # print(row_position_of_invalid_city)

    print("\nInvalid Cities from Valid States:")
    for row in row_position_of_invalid_city:
        print(f"Cell G{row[0]} - {row[1]}, {row[2]}")


def Position_of_Invalid_Zip_from_Valid_Cities(analyzer_obj):
    Invalid_Zip = Invalid_Zip_from_Valid_Cities(analyzer_obj)
    row_position_of_Invalid_Zip = analyzer_obj.Row_Position_of_Invalid_Zip_from_Valid_Cities("Test_Data_1", Invalid_Zip)
    
    print("\nInvalid Mailing Zip from Valid Cities with Valid States:")
    for row in row_position_of_Invalid_Zip:
        print(f"Cell I{row[0]} - {row[1]}, {row[2]}, {row[3]}")

def Position_of_missing_values(analyzer_obj):
    headers_mapping = analyzer_obj.get_headers_mapping("Test_Data_2", analyzer_obj.cursor)
    my_table = analyzer_obj.get_missing_table("Test_Data_2")
    position_list = analyzer_obj.find_missing_values(my_table)

    # Loop through missing values and print them in the required format
    for row_id, missing_keys in position_list:
        # Convert row_id to an integer and increment
        row_number = int(row_id) + 1  

        # Convert column names to their corresponding letters
        missing_positions = [headers_mapping[key] for key in missing_keys]

        # Concatenate letters and row ID for printing
        position_str = "".join(missing_positions) + str(row_number)  
        print(position_str)


    



def Problem_1(analyzer_obj):
    Position_of_Invalid_Names(analyzer_obj)
    Position_of_Invalid_States(analyzer_obj)
    Position_of_Invalid_Cities_from_Valid_States(analyzer_obj)
    Position_of_Invalid_Zip_from_Valid_Cities(analyzer_obj)

def main():
    analyzer_obj = AnalyzeData(db_config, api_keys)

    Problem_1(analyzer_obj)

    # Invalid_Names(analyzer_obj)
    # Invalid_States(analyzer_obj)
    # Invalid_Cities_from_Valid_States(analyzer_obj)
    # Invalid_Zip_from_Valid_Cities(analyzer_obj)

    # Position_of_Invalid_Names(analyzer_obj)
    # Position_of_Invalid_States(analyzer_obj)
    # Position_of_Invalid_Cities_from_Valid_States(analyzer_obj)
    # Position_of_Invalid_Zip_from_Valid_Cities(analyzer_obj)

    # Position_of_missing_values(analyzer_obj)


    # headers_mapping = analyzer_obj.get_headers_mapping("Test_Data_2", analyzer_obj.cursor)
    # print(headers_mapping)


if __name__ == "__main__":
    main()
