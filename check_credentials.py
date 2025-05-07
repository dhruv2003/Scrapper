import os
import json

def check_email(email):
    """Check if an email exists in any credentials file"""
    creds = {}
    credential_files = ["credentials.json", "credentials1.json", "credentials2.json"]
    
    for filename in credential_files:
        if os.path.exists(filename):
            try:
                with open(filename) as f:
                    creds.update(json.load(f))
            except Exception as e:
                print(f"Error loading {filename}: {e}")
    
    if email in creds:
        print(f"✓ Email '{email}' found in credentials")
        return True
    else:
        print(f"✗ Email '{email}' NOT found in credentials")
        available_emails = list(creds.keys())
        print(f"Available emails: {available_emails}")
        
        # Suggest similar emails
        similar = [e for e in available_emails if email[:5] in e]
        if similar:
            print(f"Similar emails found: {similar}")
            
        return False

if __name__ == "__main__":
    email = input("Enter email to check: ")
    check_email(email)
