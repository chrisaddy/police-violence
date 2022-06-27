import pandera as pa
from pandera.typing import Series

Age = pa.Field(gt=0, le=120, nullable=True)

class RawInput(pa.SchemaModel):
    age: Series[float] = Age






