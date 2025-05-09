// Configuration object defining the tables and their properties
const TABLE_CONFIG = {
  metadata: {
    TEAM_CELL: 'E5',
    DEPARTMENT_CELL: 'F5',
    MONTH_CELL: 'C5'
  },
  tables: [
    {
      name: 'Personalplanung',
      headerRow: 13,
      startRange: 'B13:G30'
    },
    {
      name: 'Client_Projects',
      headerRow: 38,
      startRange: 'B38:I63'
    },
    {
      name: 'Client_Projects_Per_Person',
      headerRow: 72,
      startRange: 'B72:Q97'
    },
    {
      name: 'Monitoring',
      headerRow: 125,
      startRange: 'B125:J142'
    }
  ]
};

// Helper function to clean headers by removing month references
function cleanHeaderName(header) {
  // Remove month patterns like (YYYY-MM) or (YYYY-M)
  return header.replace(/\s*\(\d{4}-\d{1,2}\)/g, '');
}

/**
 * Shows a dropdown dialog asking for which month to update.
 */
function showMonthSelectionDialog() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ui = SpreadsheetApp.getUi();
  
  // Get all available months from sheet names
  const sheets = ss.getSheets();
  const monthSheets = sheets.filter(sheet => {
    const sheetName = sheet.getName();
    return /^\d{4}-\d{2}$/.test(sheetName);
  }).map(sheet => sheet.getName());
  
  // If no month sheets found, alert user
  if (monthSheets.length === 0) {
    ui.alert('No monthly sheets found', 'No sheets with format YYYY-MM were found.', ui.ButtonSet.OK);
    return;
  }

  // Create HTML for dropdown selection
  const htmlOutput = HtmlService
    .createHtmlOutput(`
      <style>
        body {
          font-family: Arial, sans-serif;
          padding: 10px;
        }
        select {
          width: 100%;
          padding: 8px;
          margin-bottom: 10px;
        }
        button {
          padding: 8px 16px;
          background-color: #4285f4;
          color: white;
          border: none;
          border-radius: 4px;
          cursor: pointer;
        }
        button:hover {
          background-color: #2a75f3;
        }
      </style>
      <h3>Select Month to Update</h3>
      <form id="monthForm">
        <select id="monthSelect">
          ${monthSheets.map(sheet => `<option value="${sheet}">${sheet}</option>`).join('')}
        </select>
        <button type="submit" onclick="submitMonth()">Update</button>
      </form>
      <script>
        function submitMonth() {
          const monthSelect = document.getElementById('monthSelect');
          const selectedMonth = monthSelect.value;
          google.script.run
            .withSuccessHandler(() => google.script.host.close())
            .createAggregatedViewsForMonth(selectedMonth);
          return false;
        }
      </script>
    `)
    .setWidth(300)
    .setHeight(200);
  
  ui.showModalDialog(htmlOutput, 'Select Month to Update');
}

/**
 * Main function to update aggregated views for a specific month.
 * @param {string} selectedMonth - Month to process in format YYYY-MM.
 */
function createAggregatedViewsForMonth(selectedMonth) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // Get metadata (team, department, month)
  const team = ss.getRange(TABLE_CONFIG.metadata.TEAM_CELL).getValue();
  const department = ss.getRange(TABLE_CONFIG.metadata.DEPARTMENT_CELL).getValue();
  const plannedMonth = ss.getRange(TABLE_CONFIG.metadata.MONTH_CELL).getValue();
  
  Logger.log(`Processing sheet for Team: ${team}, Department: ${department}, Month: ${plannedMonth}, Selected Month: ${selectedMonth}`);
  
  // Process each table configuration
  TABLE_CONFIG.tables.forEach(tableConfig => {
    try {
      processTableForMonth(ss, tableConfig, selectedMonth, team, department, plannedMonth);
    } catch (error) {
      Logger.log(`Error processing table ${tableConfig.name}: ${error.toString()}`);
    }
  });
  
  // Show confirmation dialog
  SpreadsheetApp.getUi().alert(
    'Update Complete',
    `Aggregation completed for month: ${selectedMonth}`,
    SpreadsheetApp.getUi().ButtonSet.OK
  );
}

/**
 * Process a specific table for a specific month.
 */
function processTableForMonth(ss, tableConfig, selectedMonth, team, department, plannedMonth) {
  // Get aggregated view sheet for this table
  const aggregateSheetName = `Aggregated_${tableConfig.name}`;
  let aggregateSheet = ss.getSheetByName(aggregateSheetName);
  
  // If sheet doesn't exist, create it
  if (!aggregateSheet) {
    aggregateSheet = ss.insertSheet(aggregateSheetName, ss.getSheets().length);
    
    // We need to set up the headers if this is a new sheet
    // Get first monthly sheet to extract headers
    const monthlySheets = ss.getSheets().filter(sheet => /^\d{4}-\d{2}$/.test(sheet.getName()));
    
    if (monthlySheets.length === 0) {
      Logger.log(`No monthly sheets found for ${tableConfig.name}`);
      return;
    }
    
    // Get headers from the first monthly sheet
    const firstSheet = monthlySheets[0];
    const range = tableConfig.startRange.split(':')[0];
    const rangeEnd = tableConfig.startRange.split(':')[1];
    const headerRange = `${range.charAt(0)}${tableConfig.headerRow}:${rangeEnd.charAt(0)}${tableConfig.headerRow}`;
    const rawHeaders = firstSheet.getRange(headerRange).getValues()[0];
    
    // Clean the headers by removing month references
    const headers = rawHeaders.map(header => cleanHeaderName(header));
    
    // Add metadata columns to headers
    const fullHeaders = ['Year_Month', ...headers, 'Last_Updated'];
    
    // Set headers
    aggregateSheet.getRange(1, 1, 1, fullHeaders.length).setValues([fullHeaders]);
    
    // Format sheet header
    aggregateSheet.getRange(1, 1, 1, fullHeaders.length).setFontWeight('bold');
  }
  
  // Get the monthly sheet
  const monthSheet = ss.getSheetByName(selectedMonth);
  if (!monthSheet) {
    Logger.log(`Month sheet ${selectedMonth} not found`);
    return;
  }
  
  // Get headers from existing aggregation sheet
  const headers = aggregateSheet.getRange(1, 1, 1, aggregateSheet.getLastColumn()).getValues()[0];
  
  // Remove existing data for this month from the aggregation sheet if any exists
  if (aggregateSheet.getLastRow() > 1) {
    const dataRange = aggregateSheet.getRange(2, 1, aggregateSheet.getLastRow() - 1, 1);
    const yearMonthData = dataRange.getValues();
    
    // Find rows to delete (those matching selected month)
    const rowsToDelete = [];
    for (let i = 0; i < yearMonthData.length; i++) {
      if (yearMonthData[i][0] === selectedMonth) {
        // +2 because we're starting from row 2 and arrays are 0-indexed
        rowsToDelete.push(i + 2);
      }
    }
    
    // Delete rows in reverse order to not affect indices
    for (let i = rowsToDelete.length - 1; i >= 0; i--) {
      aggregateSheet.deleteRow(rowsToDelete[i]);
    }
    
    Logger.log(`Removed ${rowsToDelete.length} existing rows for month ${selectedMonth}`);
  }
  
  // Extract data from the selected month's sheet
  const dataRange = monthSheet.getRange(tableConfig.startRange);
  const data = dataRange.getValues();
  
  // Process and add new data
  let newData = [];
  for (let i = 1; i < data.length; i++) {
    if (data[i][0]) { // Only process rows with data in first column
      const row = [
        selectedMonth,  // Year_Month
        ...data[i],     // Data columns
        new Date()      // Last_Updated
      ];
      newData.push(row);
    }
  }
  
  // Insert new data at the end of the sheet
  if (newData.length > 0) {
    const lastRow = Math.max(1, aggregateSheet.getLastRow());
    aggregateSheet.getRange(lastRow + 1, 1, newData.length, headers.length)
      .setValues(newData);
    
    Logger.log(`Added ${newData.length} new rows for month ${selectedMonth}`);
    
    // Format the sheet
    formatAggregateSheet(aggregateSheet, headers.length);
  } else {
    Logger.log(`No data found for month ${selectedMonth} in table ${tableConfig.name}`);
  }
}

/**
 * Format the aggregate sheet.
 */
function formatAggregateSheet(sheet, columnCount) {
  // Auto-resize columns
  sheet.autoResizeColumns(1, columnCount);
  
  // Only add filter and format if we have data
  if (sheet.getLastRow() > 1) {
    try {
      // Check if filter exists already
      let hasFilter = false;
      try {
        sheet.getFilter();
        hasFilter = true;
      } catch (e) {
        // No filter exists
      }
      
      // Create filter if it doesn't exist
      if (!hasFilter) {
        sheet.getRange(1, 1, sheet.getLastRow(), columnCount).createFilter();
      }
      
      // Format timestamp column
      if (columnCount > 1) {
        const lastColumn = columnCount;
        const timestampColumn = sheet.getRange(2, lastColumn, Math.max(1, sheet.getLastRow() - 1), 1);
        timestampColumn.setNumberFormat('yyyy-mm-dd hh:mm:ss');
      }
      
      // Format number columns (assuming columns 5-end are numbers)
      if (columnCount > 5) {
        const numberRange = sheet.getRange(2, 5, Math.max(1, sheet.getLastRow() - 1), columnCount - 5);
        numberRange.setNumberFormat('#,##0.00');
      }
    } catch (error) {
      Logger.log(`Error formatting sheet: ${error.toString()}`);
    }
  }
}

/**
 * Legacy function to create all aggregated views.
 * Kept for backward compatibility.
 */
function createAggregatedViews() {
  // Show a dialog explaining the new feature
  const ui = SpreadsheetApp.getUi();
  const result = ui.alert(
    'Update Method Change',
    'The update process has changed. You can now select which month to update. Would you like to continue with the new method?',
    ui.ButtonSet.YES_NO
  );
  
  if (result === ui.Button.YES) {
    showMonthSelectionDialog();
  }
}

/**
 * Setup trigger for when spreadsheet is opened.
 */
function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Save')
    .addItem('Update Selected Month', 'showMonthSelectionDialog')
    .addToUi();
}