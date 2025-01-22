// Configuration object defining the tables and their properties
const TABLE_CONFIG = {
  metadata: {
    TEAM_CELL: 'E5',
    DEPARTMENT_CELL: 'F5',
    MONTH_CELL: 'C5'
  },
  // Original tables configuration
  tables: [
    {
      name: 'Personalplanung',
      headerRow: 14,
      startRange: 'B14:G31'
    },
    {
      name: 'Client_Projects',
      headerRow: 40,
      startRange: 'B40:I67'
    },
    {
      name: 'Client_Projects_Per_Content_Product',
      headerRow: 76,
      startRange: 'B76:M103'
    },
    {
      name: 'Monitoring',
      headerRow: 236,
      startRange: 'B236:J252'
    }
  ],
  // New combined projects configuration
  projectsTables: {
    name: 'Projects_Combined',
    ranges: [
      {
        type: 'UGC',
        headerRow: 111,
        startRange: 'B111:O138'
      },
      {
        type: 'Classic_Content',
        headerRow: 146,
        startRange: 'B146:O173'
      },
      {
        type: 'Strategy',
        headerRow: 181,
        startRange: 'B181:O208'
      }
    ],
    columnMappings: {
      'UGC': {
        'SMCM': 'Primary_Full_Name',
        'Asana Projects': 'Project',
        'SMCM hours done (relative)': 'Primary_Hours_Done_Relative',
        'SMCM Support': 'Support_Full_Name',
        'Support hours done (relative)': 'Support_Hours_Done_Relative',
        'SMCM substitute': 'Substitute_Full_Name',
        'Rest hours done': 'Rest_Hours_Done',
        'SMCM hours plan. (rel. in %)': 'Primary_Hours_Planned',
        'Sup. hours plan. (rel. in %)': 'Support_Hours_Planned',
        'Subs. hours plan. (rel. in %)': 'Substitute_Hours_Planned',
        'Target Hours SUM': 'Total_Target_Hours',
        'Target Hours SMCM': 'Primary_Target_Hours',
        'Target Hours Support': 'Support_Target_Hours',
        'Target Hours substitute': 'Substitute_Target_Hours'
      },
      'Classic_Content': {
        'Creative Designer': 'Primary_Full_Name',
        'Asana Projects': 'Project',
        'CD hours done (relative)': 'Primary_Hours_Done_Relative',
        'CD Support': 'Support_Full_Name',
        'Support hours done (relative)': 'Support_Hours_Done_Relative',
        'CD substitute': 'Substitute_Full_Name',
        'Rest hours done': 'Rest_Hours_Done',
        'CD hours plan. (rel. in %)': 'Primary_Hours_Planned',
        'Sup. hours plan. (rel. in %)': 'Support_Hours_Planned',
        'Subs. hours plan. (rel. in %)': 'Substitute_Hours_Planned',
        'Target Hours SUM': 'Total_Target_Hours',
        'Target Hours CD': 'Primary_Target_Hours',
        'Target Hours Support': 'Support_Target_Hours',
        'Target Hours substitute': 'Substitute_Target_Hours'
      },
      'Strategy': {
        'Creative Strategist': 'Primary_Full_Name',
        'Asana Projects': 'Project',
        'CS hours done (relative)': 'Primary_Hours_Done_Relative',
        'CS Support': 'Support_Full_Name',
        'Support hours done (relative)': 'Support_Hours_Done_Relative',
        'CS substitute': 'Substitute_Full_Name',
        'Rest hours done': 'Rest_Hours_Done',
        'CS hours plan. (rel. in %)': 'Primary_Hours_Planned',
        'Sup. hours plan. (rel. in %)': 'Support_Hours_Planned',
        'Subs. hours plan. (rel. in %)': 'Substitute_Hours_Planned',
        'Target Hours SUM': 'Total_Target_Hours',
        'Target Hours CS': 'Primary_Target_Hours',
        'Target Hours Support': 'Support_Target_Hours',
        'Target Hours substitute': 'Substitute_Target_Hours'
      }
    }
  }
};

// Helper function to clean headers by removing month references
function cleanHeaderName(header) {
  return header.replace(/\s*\(\d{4}-\d{1,2}\)/g, '').trim();
}

function createAggregatedViews() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // Get metadata
  const team = ss.getRange(TABLE_CONFIG.metadata.TEAM_CELL).getValue();
  const department = ss.getRange(TABLE_CONFIG.metadata.DEPARTMENT_CELL).getValue();
  const plannedMonth = ss.getRange(TABLE_CONFIG.metadata.MONTH_CELL).getValue();
  
  Logger.log(`Processing sheet for Team: ${team}, Department: ${department}, Month: ${plannedMonth}`);
  
  // Process original tables
  TABLE_CONFIG.tables.forEach(tableConfig => {
    try {
      processTable(ss, tableConfig, team, department, plannedMonth);
    } catch (error) {
      Logger.log(`Error processing table ${tableConfig.name}: ${error.toString()}`);
    }
  });
  
  // Process combined projects table
  try {
    processCombinedTables(ss, TABLE_CONFIG.projectsTables, team, department, plannedMonth);
  } catch (error) {
    Logger.log(`Error processing combined projects table: ${error.toString()}`);
  }
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
  
  // Get all monthly sheets
  const sheets = ss.getSheets().filter(sheet => {
    const sheetName = sheet.getName();
    return /^\d{4}_\d{2}$/.test(sheetName);
  });
  
  if (sheets.length === 0) {
    Logger.log(`No monthly sheets found for ${tableConfig.name}`);
    return;
  }
  
  // Get headers from the first sheet and clean them
  const firstSheet = sheets[0];
  const range = tableConfig.startRange.split(':')[0];
  const rangeEnd = tableConfig.startRange.split(':')[1];
  const headerRange = `${range.charAt(0)}${tableConfig.headerRow}:${rangeEnd.charAt(0)}${tableConfig.headerRow}`;
  const rawHeaders = firstSheet.getRange(headerRange).getValues()[0];
  
  // Clean the headers
  const headers = rawHeaders.map(header => cleanHeaderName(header));
  
  // Add metadata columns to headers
  const fullHeaders = ['Year_Month', ...headers, 'Last_Updated'];
  
  // Set headers
  aggregateSheet.getRange(1, 1, 1, fullHeaders.length).setValues([fullHeaders]);
  
  let allData = [];
  
  // Process each sheet
  sheets.forEach(sheet => {
    const sheetName = sheet.getName();
    if (sheetName.startsWith('Aggregated_')) return;
    
    try {
      const dataRange = sheet.getRange(tableConfig.startRange);
      const data = dataRange.getValues();
      
      // Process each row
      for (let i = 1; i < data.length; i++) {
        if (data[i][0]) { // Only process rows with data in first column
          const row = [
            sheetName,     // Year_Month
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
    
    formatAggregateSheet(aggregateSheet, fullHeaders.length);
  }
  
  Logger.log(`Processed ${allData.length} rows for ${tableConfig.name}`);
}

function processCombinedTables(ss, tableConfig, team, department, plannedMonth) {
  const aggregateSheetName = `Aggregated_${tableConfig.name}`;
  let aggregateSheet = ss.getSheetByName(aggregateSheetName);
  
  if (aggregateSheet) {
    ss.deleteSheet(aggregateSheet);
  }
  
  aggregateSheet = ss.insertSheet(aggregateSheetName, ss.getSheets().length);
  
  const sheets = ss.getSheets().filter(sheet => {
    const sheetName = sheet.getName();
    return /^\d{4}_\d{2}$/.test(sheetName);
  });
  
  if (sheets.length === 0) {
    Logger.log('No monthly sheets found');
    return;
  }
  
  const standardHeaders = [
    'Year_Month',  
    'Project_Type',
    'Role',
    'Primary_Full_Name',
    'Project',
    'Primary_Hours_Done_Relative',
    'Support_Full_Name',
    'Support_Hours_Done_Relative',
    'Substitute_Full_Name',
    'Rest_Hours_Done',
    'Primary_Hours_Planned',
    'Support_Hours_Planned',
    'Substitute_Hours_Planned',
    'Total_Target_Hours',
    'Primary_Target_Hours',
    'Support_Target_Hours',
    'Substitute_Target_Hours',
    'Last_Updated'
  ];
  
  aggregateSheet.getRange(1, 1, 1, standardHeaders.length).setValues([standardHeaders]);
  
  let allData = [];
  
  sheets.forEach(sheet => {
    const sheetName = sheet.getName();
    if (sheetName.startsWith('Aggregated_')) return;
    
    tableConfig.ranges.forEach(rangeConfig => {
      try {
        const dataRange = sheet.getRange(rangeConfig.startRange);
        const data = dataRange.getValues();
        const headers = sheet.getRange(`${rangeConfig.startRange.split(':')[0].charAt(0)}${rangeConfig.headerRow}:${rangeConfig.startRange.split(':')[1].charAt(0)}${rangeConfig.headerRow}`).getValues()[0];
        
        for (let i = 1; i < data.length; i++) {
          if (data[i][0]) {
            const mappedData = new Array(standardHeaders.length).fill('');
            
            mappedData[0] = sheetName;              // Year_Month
            mappedData[1] = rangeConfig.type;       // Project_Type
            mappedData[2] = rangeConfig.type === 'UGC' ? 'SMCM' : 
                           rangeConfig.type === 'Classic_Content' ? 'Creative Designer' :
                           'Creative Strategist';  // Role
            
            headers.forEach((header, index) => {
              const cleanHeader = cleanHeaderName(header);
              const standardHeader = tableConfig.columnMappings[rangeConfig.type][cleanHeader];
              if (standardHeader) {
                const standardIndex = standardHeaders.indexOf(standardHeader);
                if (standardIndex !== -1) {
                  mappedData[standardIndex] = data[i][index];
                }
              }
            });
            
            mappedData[mappedData.length - 1] = new Date();
            
            allData.push(mappedData);
          }
        }
      } catch (error) {
        Logger.log(`Error processing range ${rangeConfig.type} in sheet ${sheetName}: ${error.toString()}`);
      }
    });
  });
  
  if (allData.length > 0) {
    aggregateSheet.getRange(2, 1, allData.length, standardHeaders.length)
      .setValues(allData);
    
    formatAggregateSheet(aggregateSheet, standardHeaders.length);
  }
  
  Logger.log(`Processed ${allData.length} total rows for combined tables`);
}

function formatAggregateSheet(sheet, columnCount) {
  sheet.autoResizeColumns(1, columnCount);
  
  if (sheet.getLastRow() > 1) {
    try {
      sheet.getRange(1, 1, sheet.getLastRow(), columnCount).createFilter();
      
      const timestampColumn = sheet.getRange(2, columnCount, sheet.getLastRow() - 1, 1);
      timestampColumn.setNumberFormat('yyyy-mm-dd hh:mm:ss');
      
      const numberRange = sheet.getRange(2, 6, sheet.getLastRow() - 1, columnCount - 6);
      numberRange.setNumberFormat('#,##0.00');
    } catch (error) {
      Logger.log(`Error formatting sheet: ${error.toString()}`);
    }
  }
}

function onOpen() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Save')
    .addItem('Start (takes roughly 5 mins)', 'createAggregatedViews')
    .addToUi();
}