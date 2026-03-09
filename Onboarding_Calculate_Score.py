import pandas as pd
from datetime import datetime

step_name = "Calculate_Score"
execution_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
next_step = "Validate_Data"

print(f"""
Step Name: {step_name}
Execution Date: {execution_date}
Next Step: {next_step}
""")

df = pd.DataFrame({
    'Step': [step_name],
    'Timestamp': [execution_date],
    'Next': [next_step]
})

print("Sample DataFrame:\n", df)