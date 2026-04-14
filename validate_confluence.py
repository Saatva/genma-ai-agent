#!/usr/bin/env python3
"""
Validate Confluence configuration and connectivity.

Usage:
    python3 validate_confluence.py
    python3 validate_confluence.py --list-spaces
    python3 validate_confluence.py --find-page-id "Your Page Title"
"""

import os
import sys
import argparse
from dotenv import load_dotenv


def get_confluence_config():
    """Load Confluence config from .env."""
    load_dotenv()
    
    config = {
        'base_url': os.getenv('CONFLUENCE_BASE_URL', '').rstrip('/'),
        'space_key': os.getenv('CONFLUENCE_SPACE_KEY', ''),
        'username': os.getenv('CONFLUENCE_USERNAME', ''),
        'api_token': os.getenv('CONFLUENCE_API_TOKEN', ''),
        'folder_name': os.getenv('CONFLUENCE_FOLDER_NAME', 'Data Catalogs'),
        'parent_page_id': os.getenv('CONFLUENCE_PARENT_PAGE_ID', ''),
    }
    
    # Normalize empty values
    for key, val in config.items():
        if val and val.lower() in {'none', 'null', ''}:
            config[key] = None
    
    return config


def validate_basics(config):
    """Validate basic configuration."""
    print("\n" + "=" * 80)
    print("1. Basic Configuration")
    print("=" * 80)
    
    required = ['base_url', 'space_key', 'username', 'api_token']
    errors = []
    
    for field in required:
        val = config.get(field)
        if val:
            print(f"OK   {field:20s}: {val if field != 'api_token' else '***'}")
        else:
            print(f"ERR  {field:20s}: MISSING")
            errors.append(field)
    
    if errors:
        print(f"\nMissing required fields: {', '.join(errors)}")
        print("Set these in .env:")
        for field in errors:
            print(f"  {field.upper()}=value")
        return False
    
    return True


def validate_connectivity(config):
    """Test Confluence API connectivity."""
    print("\n" + "=" * 80)
    print("2. Confluence API Connectivity")
    print("=" * 80)
    
    try:
        import requests
        
        base_url = config['base_url']
        auth = (config['username'], config['api_token'])
        
        print(f"Testing connection to: {base_url}")
        
        response = requests.get(
            f"{base_url}/rest/api/space",
            params={"spaceKey": config['space_key'], "limit": 1},
            auth=auth,
            timeout=10,
        )
        
        if response.status_code == 200:
            print("Connected successfully")
            space = response.json().get("results", [{}])[0]
            if space:
                print(f"Space found: {space.get('name', config['space_key'])}")
            return True
        else:
            print(f"Connection failed with status {response.status_code}")
            print(f"  Response: {response.text[:500]}")
            
            if response.status_code == 401:
                print("  Check CONFLUENCE_USERNAME and CONFLUENCE_API_TOKEN")
            elif response.status_code == 403:
                print("  Check that user has access to space and API")
            elif response.status_code == 404:
                print("  Check CONFLUENCE_BASE_URL or CONFLUENCE_SPACE_KEY")
            
            return False
            
    except Exception as e:
        print(f"Connection error: {e}")
        print(f"  Check CONFLUENCE_BASE_URL format: {config['base_url']}")
        return False


def list_spaces(config):
    """List all accessible Confluence spaces."""
    print("\n" + "=" * 80)
    print("Available Confluence Spaces")
    print("=" * 80)
    
    try:
        import requests
        
        auth = (config['username'], config['api_token'])
        response = requests.get(
            f"{config['base_url']}/rest/api/space",
            params={"limit": 100},
            auth=auth,
            timeout=10,
        )
        
        if response.status_code != 200:
            print(f"Failed to list spaces: {response.status_code}")
            return
        
        spaces = response.json().get("results", [])
        if not spaces:
            print("No spaces found")
            return
        
        print(f"\nFound {len(spaces)} space(s):")
        for space in spaces:
            key = space.get('key', 'N/A')
            name = space.get('name', 'N/A')
            print(f"  • {name:40s} (key: {key})")
            
    except Exception as e:
        print(f"Error listing spaces: {e}")


def find_page_id(config, page_title):
    """Find a page ID by title."""
    print("\n" + "=" * 80)
    print(f"Searching for: {page_title}")
    print("=" * 80)
    
    try:
        import requests
        
        auth = (config['username'], config['api_token'])
        response = requests.get(
            f"{config['base_url']}/rest/api/content",
            params={
                "spaceKey": config['space_key'],
                "title": page_title,
                "type": "page",
                "expand": "ancestors",
            },
            auth=auth,
            timeout=10,
        )
        
        if response.status_code != 200:
            print(f"Search failed: {response.status_code}")
            return
        
        results = response.json().get("results", [])
        if not results:
            print(f"No pages found with title '{page_title}'")
            return
        
        print(f"\nFound {len(results)} page(s):")
        for page in results:
            page_id = page.get('id')
            title = page.get('title')
            ancestors = page.get('ancestors', [])
            parent_info = " > ".join([a.get('title', 'Unknown') for a in ancestors]) if ancestors else "(Space root)"
            
            print(f"\n  Page: {title}")
            print(f"     ID: {page_id}")
            print(f"     Path: {parent_info}")
            
    except Exception as e:
        print(f"Error searching for page: {e}")


def main():
    parser = argparse.ArgumentParser(
        description='Validate Confluence configuration and connectivity'
    )
    parser.add_argument('--list-spaces', action='store_true', help='List all accessible spaces')
    parser.add_argument('--find-page-id', type=str, help='Find page ID by title')
    
    args = parser.parse_args()
    
    config = get_confluence_config()
    
    print("\n" + "=" * 80)
    print("Confluence Configuration Validator")
    print("=" * 80)
    
    # Step 1: Validate basics
    if not validate_basics(config):
        sys.exit(1)
    
    # Step 2: Test connectivity
    if not validate_connectivity(config):
        sys.exit(1)
    
    print("\nConfluence configuration is valid.")
    
    # Optional: List spaces
    if args.list_spaces:
        list_spaces(config)
    
    # Optional: Find page
    if args.find_page_id:
        find_page_id(config, args.find_page_id)
    
    # Show folder page info
    print("\n" + "=" * 80)
    print("Current Target")
    print("=" * 80)
    print(f"  Folder Name: {config['folder_name']}")
    if config['parent_page_id']:
        print(f"  Parent Page ID: {config['parent_page_id']}")
        print("\n  To find your parent page ID, use:")
        print(f"     python3 validate_confluence.py --find-page-id 'Your Parent Page Title'")
    else:
        print("  Parent Page ID: (none - will create at space root)")
    
    print()


if __name__ == '__main__':
    main()
