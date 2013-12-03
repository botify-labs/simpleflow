import csv
from pandas import DataFrame

from cdf.exceptions import InvalidDataFormat


def build_dataframe_from_csv(csv_file, column_names):
    """Build a pandas DataFrame from a csv file
    csv_file_path : a file object from a csv file
    column_names : a list of string representing
                   the field names in the csv file.
                   the result DataFrame columns are named
                   after this list
    Return a pandas Dataframe which rows are the csv file lines
    and which columns are the different fields in the csv file

    Raise a InvalidDataFormat exception if the format of the csv file
    is incoherent with column_names
    """
    csv_reader = csv.reader(csv_file,
                            delimiter="\t",
                            quotechar=None,
                            quoting=csv.QUOTE_NONE)
    row_list = [row for row in csv_reader]

    #check format
    if not all([len(row) == len(column_names) for row in row_list]):
        sample_error_line = next(row for row in row_list
                                 if len(row) != len(column_names))
        if hasattr(csv_file, "name"):
            filename = csv_file.name
        else:
            filename = "Unknown source"
        raise InvalidDataFormat("'%s' has incorrect format. "
                                "Each row should contain exactly %d elements. "
                                "Sample error line : '%s'"
                                % (filename, len(column_names), sample_error_line))

    if len(row_list) > 0:
        return DataFrame(row_list, columns=column_names)
    else:
        return DataFrame(columns=column_names)
