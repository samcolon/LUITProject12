import boto3

# Declare profile name
session = boto3.Session(profile_name='leveluptech')

# Initialize a DynamoDB client
dynamodb = session.client(
    'dynamodb'
    #aws_access_key_id='*****',
    #aws_secret_access_key='*****',
    )

# Specify the table name
table_name = 'MediaCatalog'

# Define the scan parameters
scan_params = {
    'TableName': table_name,
}

# Perform the scan operation
response = dynamodb.scan(**scan_params)

# Print the scanned items
items = response.get('Items', [])
for item in items:
    movie_title = item.get('MovieTitle', {}).get('S', 'N/A')
    genre = item.get('Genre', {}).get('S', 'N/A')
    rating = item.get('Rating', {}).get('S', 'N/A')
    release_date = item.get('Release Date', {}).get('S', 'N/A')
    
    print(f"Movie Title: {movie_title}")
    print(f"Genre: {genre}")
    print(f"Rating: {rating}")
    print(f"Release Date: {release_date}")
    print("-" * 20)

# Check if there are more items to scan
while 'LastEvaluatedKey' in response:
    scan_params['ExclusiveStartKey'] = response['LastEvaluatedKey']
    response = dynamodb.scan(**scan_params)
    
    # Print the scanned items in the additional scan
    items = response.get('Items', [])
    for item in items:
        movie_title = item.get('MovieTitle', {}).get('S', 'N/A')
        genre = item.get('Genre', {}).get('S', 'N/A')
        rating = item.get('Rating', {}).get('S', 'N/A')
        release_date = item.get('Release Date', {}).get('S', 'N/A')

        print(f"Movie Title: {movie_title}")
        print(f"Genre: {genre}")
        print(f"Rating: {rating}")
        print(f"Release Date: {release_date}")
        print("-" * 20)
