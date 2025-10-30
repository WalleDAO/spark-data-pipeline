```markdown
# Dune Analytics & Arkham Intelligence Integration Tool

This tool integrates Dune Analytics and Arkham Intelligence APIs to fetch cryptocurrency addresses, enrich them with labels, and store the results in a Dune table for analysis.

## Features

- 🔍 **Data Extraction**: Fetch address data from Dune Analytics
- 🏷️ **Label Enrichment**: Get comprehensive labels from Arkham Intelligence
- 🗃️ **Data Management**: Automatically clear and update Dune tables
- 📊 **Batch Processing**: Handle large datasets efficiently with concurrent processing
- ⚡ **Rate Limiting**: Built-in delays to respect API limits

## Workflow

The tool executes in 4 main steps:

1. **Step 1**: Fetch address data from Dune Analytics
2. **Step 2**: Get address labels through Arkham Intelligence API
3. **Step 3**: Clear existing data table contents
4. **Step 4**: Insert processed data back to Dune platform

## Prerequisites

- Python 3.7+
- Dune Analytics API key
- Arkham Intelligence API access
- Required Python packages (see requirements.txt)

## Installation

1. **Clone the repository**
   ```bash
   git clone https://github.com/yourusername/dune-arkham-integration.git
   cd dune-arkham-integration
   ```

2. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure the application**
   ```bash
   cp config.example.py config.py
   ```

4. **Edit config.py with your actual values**

## Configuration

Edit `config.py` and fill in the following required values:

### Dune Analytics Configuration
- `DUNE_API_KEY`: Your Dune Analytics API key
- `DUNE_TABLE_ID`: The ID of your Dune table for data operations
- `DUNE_TABLE_NAME_SPACE`: Your table namespace (for table creation)
- `DUNE_TABLE_NAME`: Your table name (for table creation)
- `DUNE_TABLE_DESCRIPTION`: Description for your table (for table creation)

### Arkham Intelligence Configuration
- `ARKHAM_URL`: Arkham Intelligence API endpoint URL
- `ARKHAM_API_KEY`: Your Arkham Intelligence API key
- `MAX_WORKERS`: Maximum concurrent threads (default: 10)
- `REQUEST_DELAY`: Delay between requests in seconds (default: 0.05)

## Usage

### Basic Usage
Run the main integration process:
```bash
python main.py
```

### Create Dune Table (Optional)
If you need to create a new Dune table first:
```bash
python createDuneTable.py
```

## Project Structure

```
├── main.py                 # Main execution script
├── getDuneData.py         # Fetch data from Dune Analytics
├── getArkHamLabels.py     # Get labels from Arkham Intelligence
├── clearDuneTable.py      # Clear existing Dune table data
├── insertDuneTable.py     # Insert data to Dune table
├── createDuneTable.py     # Create new Dune table (optional)
├── config.example.py      # Configuration template
├── config.py             # Your actual configuration (not tracked)
├── requirements.txt      # Python dependencies
├── .gitignore           # Git ignore rules
└── README.md           # This file
```

## Data Schema

The tool creates/uses a Dune table with the following schema:

| Column | Type | Description |
|--------|------|-------------|
| no | integer | Record number |
| address | varbinary | Cryptocurrency address |
| name | varchar | Entity name |
| type | varchar | Entity type |
| label | varchar | Address label |
| isuseraddress | boolean | Whether it's a user address |
| website | varchar | Associated website |
| twitter | varchar | Twitter handle |
| crunchbase | varchar | Crunchbase profile |
| linkedin | varchar | LinkedIn profile |

## Error Handling

The tool includes comprehensive error handling:
- ✅ Each step validates success before proceeding
- ❌ Program terminates gracefully if any step fails
- 📝 Detailed error messages for troubleshooting
- 🔄 Built-in retry mechanisms for API calls

## Rate Limiting

To respect API limits:
- Configurable concurrent workers (`MAX_WORKERS`)
- Adjustable request delays (`REQUEST_DELAY`)
- Automatic backoff on rate limit errors

## Output

- **Console**: Real-time progress updates and status messages
- **Files**: Processed data saved to timestamped CSV files
- **Dune Table**: Updated table ready for analysis and visualization

## Troubleshooting

### Common Issues

1. **API Key Issues**
   - Verify your API keys are correct and active
   - Check API key permissions and quotas

2. **Table Not Found**
   - Ensure `DUNE_TABLE_ID` is correct
   - Run `createDuneTable.py` to create a new table if needed

3. **Rate Limiting**
   - Increase `REQUEST_DELAY` value
   - Decrease `MAX_WORKERS` value

4. **Network Issues**
   - Check internet connection
   - Verify API endpoints are accessible

### Debug Mode

For detailed debugging information, modify the logging level in your scripts or add print statements as needed.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

[Add your license information here]

## Support

For issues and questions:
- Create an issue on GitHub
- Check the troubleshooting section above
- Review API documentation for Dune Analytics and Arkham Intelligence

## Disclaimer

This tool is for educational and research purposes. Please ensure you comply with the terms of service of both Dune Analytics and Arkham Intelligence APIs.
```