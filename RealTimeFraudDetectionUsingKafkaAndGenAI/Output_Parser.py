# Output_Parser.py
from pydantic import BaseModel, Field
from langchain.output_parsers import PydanticOutputParser

# Define schema
class FraudResult(BaseModel):
    transaction_id: str = Field(..., description="Unique transaction ID")
    decision: str = Field(..., description="Fraud or Valid")
    description: str = Field(..., description="Description of why the transaction is fraud/valid")

# Create parser
parser = PydanticOutputParser(pydantic_object=FraudResult)

# Function to parse
def analyze_transaction(llm_output: str) -> FraudResult:
    """
    Parse the raw LLM JSON output into a FraudResult object.
    """
    return parser.parse(llm_output)
