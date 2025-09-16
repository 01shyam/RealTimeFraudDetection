import streamlit as st
import pandas as pd
from langchain_core.prompts import PromptTemplate
from langchain_ollama import ChatOllama

# Initialize LLM
llm = ChatOllama(model="llama3")  # Make sure llama3 is running locally

# ------------------ Rule Based Fraud Function ------------------
def rule_based_fraud(row):
    score = 0
    if row["Transaction_Amount"] > 5000:
        score += 3
    if row["Transaction_Frequency"] > 2 * row["Hist_Trans_Frequency"]:
        score += 2
    hour = row["Transaction_Time"].hour
    if hour < 6 or hour >= 23:
        score += 2
    if row["Txn_In_Foreign_Country"] == 1:
        score += 3
    if row["Declined_Transactions"] >= 4:
        score += 2

    return "Fraud" if score > 5 else "Valid"

# ------------------ LLM Explanation Function ------------------
def explain_fraud(txn, rule_decision):
    txn_text = "\n".join([f"{k}: {v}" for k, v in txn.items()])
    prompt = PromptTemplate(
        input_variables=["txn_text", "rule_decision"],
        template="""
You are a fraud analyst. 
A rule-based system has already analyzed this transaction and concluded:
Decision: {rule_decision}

Now, as an expert, provide reasoning in 2–3 sentences on why this decision makes sense 
(based on the transaction details). Be concise, and highlight the risk factors.

Transaction details:
{txn_text}
"""
    )
    chain = prompt | llm
    response = chain.invoke({"txn_text": txn_text, "rule_decision": rule_decision})

    return response.content

# ------------------ Streamlit App ------------------



def fraud_detect (consumed_txn):

        result_lst = {
        'Transaction_ID': [],
        'Account_ID': [],
        'Fraud_Result': [],
        'Review_Summary': []

          }

        decision = rule_based_fraud(consumed_txn)
        llm_explanation = explain_fraud(consumed_txn, decision)
        result_lst['Transaction_ID'].append(consumed_txn['Transaction_ID'])
        result_lst['Account_ID'].append(consumed_txn['Account_ID'])
        result_lst['Fraud_Result'].append(decision)
        result_lst['Review_Summary'].append(llm_explanation)

        return result_lst
