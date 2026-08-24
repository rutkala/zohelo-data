#!/usr/bin/env python3
"""
Google OAuth Refresh Token Generator for GitHub Actions
Works in Google Colab or locally with CLI
"""

import json
import sys

try:
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("❌ Installing required packages...")
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "google-auth-oauthlib", "google-auth-httplib2", "-q"])
    from google_auth_oauthlib.flow import InstalledAppFlow

SCOPES = ['https://www.googleapis.com/auth/drive']

def main():
    print("\n" + "="*80)
    print("GOOGLE OAUTH REFRESH TOKEN GENERATOR")
    print("="*80 + "\n")
    
    # Get OAuth Client JSON
    user_input = input("Paste your OAuth Client JSON: ").strip()
    
    try:
        oauth_data = json.loads(user_input)
    except json.JSONDecodeError as e:
        print(f"❌ Invalid JSON: {e}")
        return
    
    # Extract config from nested structure if needed
    if 'installed' in oauth_data:
        config = oauth_data['installed']
        print("✅ Desktop OAuth Client detected")
    else:
        config = oauth_data
    
    # Validate
    if 'client_id' not in config or 'client_secret' not in config:
        print(f"❌ Missing credentials. Found keys: {list(config.keys())}")
        return
    
    print("✅ OAuth config loaded successfully\n")
    
    try:
        # Create flow
        flow = InstalledAppFlow.from_client_config(config, scopes=SCOPES)
        
        # Generate auth URL
        auth_url, _ = flow.authorization_url(prompt='consent')
        
        print("="*80)
        print("STEP 1: CLICK THIS LINK TO AUTHORIZE")
        print("="*80)
        print(f"\n{auth_url}\n")
        print("="*80)
        print("After authorizing, copy the authorization code from the URL")
        print("="*80 + "\n")
        
        # Get auth code
        auth_code = input("Paste authorization code: ").strip()
        if not auth_code:
            print("❌ No code provided")
            return
        
        print("\n⏳ Exchanging code for refresh token...\n")
        
        # Exchange code
        credentials = flow.fetch_token(authorization_response=f"http://localhost/?code={auth_code}")
        
        refresh_token = credentials.get('refresh_token')
        if not refresh_token:
            print("❌ No refresh token received")
            print("   Try revoking permissions at: https://myaccount.google.com/permissions")
            return
        
        print("="*80)
        print("✅ SUCCESS!")
        print("="*80)
        print(f"\n🔑 REFRESH TOKEN:\n\n{refresh_token}\n")
        print("="*80)
        print("NEXT: Add this to GitHub Secrets")
        print("="*80)
        print("\n1. Go to: https://github.com/rutkala/zohelo-data/settings/secrets/actions")
        print("2. Click 'New repository secret'")
        print("3. Name: GOOGLE_OAUTH_REFRESH_TOKEN")
        print("4. Value: [paste token above]")
        print("5. Click 'Add secret'\n")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
