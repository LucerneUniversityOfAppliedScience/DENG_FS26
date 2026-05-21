from pyspark.sql.functions import udf
from pyspark.sql.types import BooleanType
import re


@udf(returnType=BooleanType())
def is_valid_email(email):
    pattern = r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"
    if email is None:
        return False
    return re.match(pattern, email) is not None
