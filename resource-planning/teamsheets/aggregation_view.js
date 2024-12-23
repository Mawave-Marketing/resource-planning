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
        headerRow: 14,
        startRange: 'B14:G26'
      },
      {
        name: 'Client_Projects',
        headerRow: 34,
        startRange: 'B34:I61'
      },
      {
        name: 'Client_Projects_Per_Person',
        headerRow: 70,
        startRange: 'B70:N98'
      },
      {
        name: 'Monitoring',
        headerRow: 125,
        startRange: 'B125:J135'
      }
    ]
  };
  
  function createAggregatedViews() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // Get metadata (team, department, month)
    const team = ss.getRange(TABLE_CONFIG.metadata.TEAM_CELL).getValue();
    const department = ss.getRange(TABLE_CONFIG.metadata.DEPARTMENT_CELL).getValue();
    const plannedMonth = ss.getRange(TABLE_CONFIG.metadata.MONTH_CELL).getValue();
    
    Logger.log(`Processing sheet for Team: ${team}, Department: ${department}, Month: ${plannedMonth}`);
    
    // Process each table configuration
    TABLE_CONFIG.tables.forEach(tableConfig => {
      try {
        processTable(ss, tableConfig, team, department, plannedMonth);
      } catch (error) {
        Logger.log(`Error processing table ${tableConfig.name}: ${error.toString()}`);
      }
    });
  }
  
  function processTable(ss, tableConfig, team, department, plannedMonth) {
    // Get or create aggregated view sheet for this table
    const aggregateSheetName = `Aggregated_${tableConfig.name}`;
    let aggregateSheet = ss.getSheetByName(aggregateSheetName);
    
    // If sheet exists, remove it
    if (aggregateSheet) {
      ss.deleteSheet(aggregateSheet);
    }
    
    // Create new sheet at the end
    aggregateSheet = ss.insertSheet(aggregateSheetName, ss.getSheets().length);
    
    // Get all monthly sheets (YYYY_MM pattern)
    const sheets = ss.getSheets().filter(sheet => {
      const sheetName = sheet.getName();
      return /^\d{4}_\d{2}$/.test(sheetName);
    });
    
    // Process first sheet to get headers
    if (sheets.length === 0) {
      Logger.log(`No monthly sheets found for ${tableConfig.name}`);
      return;
    }
    
    // Get headers from the first sheet
    const firstSheet = sheets[0];
    const range = tableConfig.startRange.split(':')[0];
    const rangeEnd = tableConfig.startRange.split(':')[1];
    const headerRange = `${range.charAt(0)}${tableConfig.headerRow}:${rangeEnd.charAt(0)}${tableConfig.headerRow}`;
    const headers = firstSheet.getRange(headerRange).getValues()[0];
    
    // Add metadata columns to headers
    const fullHeaders = ['Year_Month', 'Team', 'Department', ...headers, 'Last_Updated'];
    
    // Set headers
    aggregateSheet.getRange(1, 1, 1, fullHeaders.length).setValues([fullHeaders]);
    
    let allData = [];
    
    // Process each sheet
    sheets.forEach(sheet => {
      const sheetName = sheet.getName();
      if (sheetName.startsWith('Aggregated_')) return;
      
      try {
        // Get data range (excluding header row)
        const dataRange = sheet.getRange(tableConfig.startRange);
        const data = dataRange.getValues();
        
        // Process each row
        for (let i = 1; i < data.length; i++) {
          if (data[i][0]) { // Only process rows with data in first column
            const row = [
              sheetName,     // Year_Month
              team,          // Team
              department,    // Department
              ...data[i],    // Data columns
              new Date()     // Last_Updated
            ];
            allData.push(row);
          }
        }
      } catch (error) {
        Logger.log(`Error processing sheet ${sheetName}: ${error.toString()}`);
      }
    });
    
    // Write data if we have any
    if (allData.length > 0) {
      aggregateSheet.getRange(2, 1, allData.length, fullHeaders.length)
        .setValues(allData);
      
      // Format
      formatAggregateSheet(aggregateSheet, fullHeaders.length);
    }
    
    Logger.log(`Processed ${allData.length} rows for ${tableConfig.name}`);
  }
  
  function formatAggregateSheet(sheet, columnCount) {
    // Auto-resize columns
    sheet.autoResizeColumns(1, columnCount);
    
    // Only add filter and format if we have data
    if (sheet.getLastRow() > 1) {
      try {
        // Add filter
        sheet.getRange(1, 1, sheet.getLastRow(), columnCount).createFilter();
        
        // Format timestamp column
        const timestampColumn = sheet.getRange(2, columnCount, sheet.getLastRow() - 1, 1);
        timestampColumn.setNumberFormat('yyyy-mm-dd hh:mm:ss');
        
        // Format number columns (assuming columns 5-end are numbers)
        const numberRange = sheet.getRange(2, 5, sheet.getLastRow() - 1, columnCount - 5);
        numberRange.setNumberFormat('#,##0.00');
      } catch (error) {
        Logger.log(`Error formatting sheet: ${error.toString()}`);
      }
    }
  }
  
  function onOpen() {
    const ui = SpreadsheetApp.getUi();
    ui.createMenu('Capacity Planning')
      .addItem('Aggregate All Tables', 'createAggregatedViews')
      .addToUi();
  }