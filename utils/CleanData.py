
class CleanData:

    def __init__(self):
        return
    def clean_name(name_tuple):
        """
        Cleans name data by trimming whitespace, capitalizing first letters of
        each name part, and handling missing middle initials.

        :param name_tuple: A tuple containing the first name, middle initial, and last name.
        :return: A cleaned, formatted string of the full name.
        """
        # Unpack the tuple into variables
        first_name, middle_initial, last_name = name_tuple

        # Trim whitespace
        first_name = first_name.strip().capitalize()
        last_name = last_name.strip().capitalize()

        # Check if middle initial is present and handle accordingly
        if middle_initial:
            middle_initial = middle_initial.strip().upper()
            # Construct the full name with the middle initial
            full_name = f"{first_name} {middle_initial}. {last_name}"
        else:
            # Construct the full name without the middle initial
            full_name = f"{first_name} {last_name}"

        return full_name

    def filter_name_data(name):
        return
