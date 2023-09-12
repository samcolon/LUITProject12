import boto3

# Declare profile name
session = boto3.Session(profile_name='enter_profile_name_here')

# Initialize a DynamoDB client
dynamodb = session.client(
    'dynamodb'
    #aws_access_key_id='*****',
    #aws_secret_access_key='*****',
    )

# Specify the table name
table_name = 'MediaCatalog'

# Define the items you want to add
items = [
    {
        'MovieTitle': {'S': 'Weird Science'},
        'Genre': {'S': 'Comedy'},
        'Rating': {'S': 'PG-13'},
        'Release Date': {'S': '1985-08-02'},
    },
    {
        'MovieTitle': {'S': 'Short Circuit'},
        'Genre': {'S': 'Sci-Fi'},
        'Rating': {'S': 'PG'},
        'Release Date': {'S': '1986-05-09'},
    },
    {
        'MovieTitle': {'S': 'Friday the 13th'},
        'Genre': {'S': 'Horror'},
        'Rating': {'S': 'R'},
        'Release Date': {'S': '1980-05-09'},
    },
    {
        'MovieTitle': {'S': 'Coming to America'},
        'Genre': {'S': 'Comedy'},
        'Rating': {'S': 'R'},
        'Release Date': {'S': '1988-06-26'},
    },
    {
        'MovieTitle': {'S': 'The Land Before Time'},
        'Genre': {'S': 'Family'},
        'Rating': {'S': 'G'},
        'Release Date': {'S': '1988-11-18'},
    },
    {
        'MovieTitle': {'S': 'Willow'},
        'Genre': {'S': 'Fantasy'},
        'Rating': {'S': 'PG'},
        'Release Date': {'S': '1988-05-20'},
    },
    {
        'MovieTitle': {'S': 'Top Gun'},
        'Genre': {'S': 'Action'},
        'Rating': {'S': 'PG'},
        'Release Date': {'S': '1986-05-16'},
    },
    {
        'MovieTitle': {'S': 'BeetleJuice'},
        'Genre': {'S': 'Fantasy'},
        'Rating': {'S': 'PG'},
        'Release Date': {'S': '1988-03-30'},
    },
    {
        'MovieTitle': {'S': 'Beverly Hills Cop'},
        'Genre': {'S': 'Comedy'},
        'Rating': {'S': 'R'},
        'Release Date': {'S': '1984-12-05'},
    },
    {
        'MovieTitle': {'S': 'Commando'},
        'Genre': {'S': 'Action'},
        'Rating': {'S': 'R'},
        'Release Date': {'S': '1985-10-04'},
    },
]

# Create a list of PutRequests for batch writing
put_requests = [{'PutRequest': {'Item': item}} for item in items]

# Split the put_requests list into batches of 25 items (DynamoDB batch write limit)
for batch in [put_requests[i:i + 25] for i in range(0, len(put_requests), 25)]:
    request_items = {table_name: batch}
    
    # Perform batch write operation
    response = dynamodb.batch_write_item(RequestItems=request_items)
    
    # Check if any unprocessed items
    while 'UnprocessedItems' in response:
        # Retry with the unprocessed items if any
        unprocessed_items = response['UnprocessedItems']
        if not unprocessed_items:
            break  # No more unprocessed items, exit the loop
        request_items = {table_name: unprocessed_items}
        response = dynamodb.batch_write_item(RequestItems=request_items)

# Check if all items were successfully added
if not response.get('UnprocessedItems'):
    print("All items added successfully")
else:
    print("Some items were not added successfully")