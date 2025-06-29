#!/usr/bin/env python3
"""
Test script to verify duplicate document prevention functionality.
This script tests that documents cannot be uploaded twice, but can be re-uploaded after deletion.
"""

import sys
import os
import requests
import json
import time

# Add the backend directory to the path so we can import our modules
sys.path.append(os.path.join(os.path.dirname(__file__), 'backend'))

from backend.app.rag.embed_db import verify_vectors_for_filename

def test_duplicate_prevention():
    """Test that duplicate documents are prevented from uploading."""
    
    # Configuration
    API_BASE_URL = "http://localhost:8000/api"
    USER_ID = "user_123456"
    TEST_FILENAME = "test_duplicate.txt"
    
    print("=== Duplicate Document Prevention Test ===")
    print(f"Testing duplicate prevention for user: {USER_ID}")
    print(f"Testing with file: {TEST_FILENAME}")
    print()
    
    # Step 1: Create a test file
    print("Step 1: Creating test file...")
    test_file_path = f"storage/documents/{USER_ID}/{TEST_FILENAME}"
    os.makedirs(f"storage/documents/{USER_ID}", exist_ok=True)
    
    with open(test_file_path, 'w') as f:
        f.write("This is a test document for duplicate prevention testing.")
    
    print(f"✅ Test file created: {test_file_path}")
    print()
    
    # Step 2: Upload the file for the first time
    print("Step 2: Uploading file for the first time...")
    try:
        with open(test_file_path, 'rb') as f:
            files = {'documents': (TEST_FILENAME, f, 'text/plain')}
            data = {'user_id': USER_ID}
            
            response = requests.post(f"{API_BASE_URL}/upload", files=files, data=data)
            result = response.json()
            
            if response.status_code == 200:
                print(f"✅ First upload successful: {result.get('detail', '')}")
                print(f"   - Uploaded count: {result.get('uploaded_count', 0)}")
                print(f"   - Skipped files: {len(result.get('skipped_files', []))}")
            else:
                print(f"❌ First upload failed: {result.get('detail', 'Unknown error')}")
                return False
                
    except Exception as e:
        print(f"❌ Error during first upload: {str(e)}")
        return False
    
    print()
    
    # Step 3: Wait for processing to complete
    print("Step 3: Waiting for processing to complete...")
    time.sleep(10)  # Give time for embedding to complete
    
    # Step 4: Try to upload the same file again (should be prevented)
    print("Step 4: Attempting to upload the same file again (should be prevented)...")
    try:
        with open(test_file_path, 'rb') as f:
            files = {'documents': (TEST_FILENAME, f, 'text/plain')}
            data = {'user_id': USER_ID}
            
            response = requests.post(f"{API_BASE_URL}/upload", files=files, data=data)
            result = response.json()
            
            if response.status_code == 200:
                uploaded_count = result.get('uploaded_count', 0)
                skipped_files = result.get('skipped_files', [])
                
                if uploaded_count == 0 and len(skipped_files) > 0:
                    print("✅ Duplicate prevention working correctly!")
                    print(f"   - Uploaded count: {uploaded_count}")
                    print(f"   - Skipped files: {len(skipped_files)}")
                    for skipped in skipped_files:
                        print(f"   - Skipped: {skipped.get('filename')} - {skipped.get('reason')}")
                else:
                    print("❌ Duplicate prevention failed!")
                    print(f"   - Uploaded count: {uploaded_count}")
                    print(f"   - Skipped files: {len(skipped_files)}")
                    return False
            else:
                print(f"❌ Second upload request failed: {result.get('detail', 'Unknown error')}")
                return False
                
    except Exception as e:
        print(f"❌ Error during second upload: {str(e)}")
        return False
    
    print()
    
    # Step 5: Delete the file
    print("Step 5: Deleting the file...")
    try:
        response = requests.delete(f"{API_BASE_URL}/documents/{USER_ID}/{TEST_FILENAME}")
        
        if response.status_code == 200:
            print(f"✅ File deletion successful: {response.json().get('detail', '')}")
        else:
            print(f"❌ File deletion failed: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ Error during file deletion: {str(e)}")
        return False
    
    print()
    
    # Step 6: Wait a moment for deletion to complete
    print("Step 6: Waiting for deletion to complete...")
    time.sleep(3)
    
    # Step 7: Try to upload the same file again (should be allowed now)
    print("Step 7: Attempting to upload the same file again after deletion (should be allowed)...")
    try:
        with open(test_file_path, 'rb') as f:
            files = {'documents': (TEST_FILENAME, f, 'text/plain')}
            data = {'user_id': USER_ID}
            
            response = requests.post(f"{API_BASE_URL}/upload", files=files, data=data)
            result = response.json()
            
            if response.status_code == 200:
                uploaded_count = result.get('uploaded_count', 0)
                skipped_files = result.get('skipped_files', [])
                
                if uploaded_count > 0:
                    print("✅ Re-upload after deletion successful!")
                    print(f"   - Uploaded count: {uploaded_count}")
                    print(f"   - Skipped files: {len(skipped_files)}")
                else:
                    print("❌ Re-upload after deletion failed!")
                    print(f"   - Uploaded count: {uploaded_count}")
                    print(f"   - Skipped files: {len(skipped_files)}")
                    return False
            else:
                print(f"❌ Re-upload request failed: {result.get('detail', 'Unknown error')}")
                return False
                
    except Exception as e:
        print(f"❌ Error during re-upload: {str(e)}")
        return False
    
    print()
    
    # Step 8: Clean up
    print("Step 8: Cleaning up test file...")
    try:
        if os.path.exists(test_file_path):
            os.remove(test_file_path)
            print("✅ Test file cleaned up")
    except Exception as e:
        print(f"⚠️  Warning: Could not clean up test file: {str(e)}")
    
    print()
    print("✅ All tests completed successfully!")
    return True

def test_vector_verification():
    """Test the vector verification functionality."""
    
    TEST_FILENAME = "test_duplicate.txt"
    
    print("=== Vector Verification Test ===")
    print(f"Testing vector verification for: {TEST_FILENAME}")
    print()
    
    # Check if vectors exist
    verification_result = verify_vectors_for_filename(
        filename=TEST_FILENAME,
        collection_name="my_collection",
        model_name="Snowflake/snowflake-arctic-embed-l-v2.0",
        milvus_host="127.0.0.1",
        milvus_port=19530
    )
    
    print(f"Verification result: {json.dumps(verification_result, indent=2)}")
    print()
    
    matching_docs = verification_result.get("matching_documents", 0)
    if matching_docs > 0:
        print(f"✅ Found {matching_docs} matching documents in vector database")
    else:
        print("ℹ️  No matching documents found in vector database")
    
    return True

if __name__ == "__main__":
    print("Duplicate Document Prevention Test Suite")
    print("=" * 60)
    print()
    
    # Test vector verification
    print("Testing vector verification...")
    test_vector_verification()
    print()
    
    # Test duplicate prevention
    print("Testing duplicate prevention...")
    success = test_duplicate_prevention()
    print()
    
    if success:
        print("✅ All tests passed! Duplicate prevention is working correctly.")
    else:
        print("❌ Some tests failed! Please check the implementation.")
    
    print()
    print("=" * 60)
    print("Test completed!") 