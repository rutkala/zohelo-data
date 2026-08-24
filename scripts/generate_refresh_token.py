#!/usr/bin/env python3
"""
Google OAuth Refresh Token Generator
Run this script to generate a refresh token for GitHub Actions

Usage:
1. Make sure your OAuth Client JSON is saved
2. Run: python generate_refresh_token.py
3. Follow the prompts
"""

import json
import sys
import os
from pathlib import Path

try:
    from google.auth.transport.requests import Request
    from google_auth_oauthlib.flow import InstalledAppFlow
except ImportError:
    print("❌ Required packages not installed!")
    print("Run: pip install google-auth-oauthlib google-auth-httplib2")
    sys.exit(1)

SCOPES = ['https://www.googleapis.com/auth/drive']

def main():
    print("\n" + "="*80)
    print("GOOGLE OAUTH REFRESH TOKEN GENERATOR")
    print("="*80)
    print("\nThis tool will help you generate a refresh token for GitHub Actions.\n")
    
    # Ask for OAuth Client JSON
    print("📝 Please provide your OAuth Client JSON:")
    print("   Option 1: Enter the file path to your JSON file")
    print("   Option 2: Paste the JSON content directly\n")
    
    user_input = input("Enter file path or JSON (starts with '{' if pasting): ").strip()
    
    # Parse input
    try:
        if user_input.startswith('{'):
            # User pasted JSON
            oauth_config = json.loads(user_input)
        else:
            # User provided file path
            with open(user_input, 'r') as f:
                oauth_config = json.load(f)
        print("✅ OAuth Client JSON loaded successfully\n")
    except json.JSONDecodeError as e:
        print(f"❌ Error parsing JSON: {e}")
        sys.exit(1)
    except FileNotFoundError:
        print(f"❌ File not found: {user_input}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)
    
    # Validate OAuth config
    if 'client_id' not in oauth_config or 'client_secret' not in oauth_config:
        print("❌ Invalid OAuth Client JSON: missing client_id or client_secret")
        sys.exit(1)
    
    try:
        # Create flow
        flow = InstalledAppFlow.from_client_config(oauth_config, scopes=SCOPES)
        
        # Generate authorization URL
        auth_url, _ = flow.authorization_url(prompt='consent')
        
        print("="*80)
        print("STEP 1: AUTHORIZE GOOGLE DRIVE ACCESS")
        print("="*80)
        print("\n🔗 Click this link (or paste in browser):\n")
        print(auth_url)
        print("\n" + "-"*80)
        print("After clicking the link:")
        print("1. Sign in with your Google account (R.Utkala@gmail.com)")
        print("2. Click 'Allow' when asked for permission")
        print("3. You'll be redirected to localhost")
        print("4. Copy the 'code' parameter from the URL")
        print("-"*80 + "\n")
        
        # Wait for authorization code
        auth_code = input("Paste the authorization code here: ").strip()
        
        if not auth_code:
            print("❌ No authorization code provided")
            sys.exit(1)
        
        print("\n⏳ Exchanging code for refresh token...")
        
        # Exchange code for tokens
        credentials = flow.fetch_token(authorization_response=f"http://localhost:8080/?code={auth_code}")
        
        refresh_token = credentials.get('refresh_token')
        
        if not refresh_token:
            print("❌ No refresh token received. This might happen if:")
            print("   - You already authorized this app before")
            print("   - You need to revoke permission and try again")
            print("\nTo revoke: https://myaccount.google.com/permissions")
            sys.exit(1)
        
        print("\n" + "="*80)
        print("✅ SUCCESS! REFRESH TOKEN GENERATED")
        print("="*80)
        print("\n🔑 Your Refresh Token:\n")
        print(refresh_token)
        print("\n" + "="*80)
        print("NEXT STEPS:")
        print("="*80)
        print("\n1. Copy the refresh token above")
        print("2. Go to: https://github.com/rutkala/zohelo-data/settings/secrets/actions")
        print("3. Click 'New repository secret'")
        print("4. Name: GOOGLE_OAUTH_REFRESH_TOKEN")
        print("5. Value: Paste your refresh token")
        print("6. Click 'Add secret'")
        print("\n7. Your workflows will now authenticate successfully! 🎉\n")
        
    except Exception as e:
        print(f"❌ Error during authorization: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
