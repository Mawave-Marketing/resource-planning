function protectFormulasBySheetPattern() {
  // ===== CONFIGURATION SECTION =====
  // List of users who should be able to edit protected formula cells
  const allowedEditors = [
    'simon.heinken@mawave.de',
    'jassen@mawave.de'
  ];
  
  // Define the pattern for sheet names (YYYY-MM)
  const sheetPattern = /^\d{4}-\d{2}$/;
  
  // Get current date to filter sheets
  const now = new Date();
  const currentYear = now.getFullYear();
  const currentMonth = now.getMonth() + 1; // JavaScript months are 0-indexed
  // ===== END CONFIGURATION SECTION =====
  
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  const me = Session.getEffectiveUser();
  
  // Process only sheets matching our pattern and date criteria
  sheets.forEach(sheet => {
    const sheetName = sheet.getName();
    
    // Skip sheets that don't match our YYYY-MM pattern
    if (!sheetPattern.test(sheetName)) {
      Logger.log(`Skipping sheet: ${sheetName} (doesn't match pattern)`);
      return;
    }
    
    // Parse year and month from sheet name
    const [yearStr, monthStr] = sheetName.split('-');
    const sheetYear = parseInt(yearStr, 10);
    const sheetMonth = parseInt(monthStr, 10);
    
    // Skip sheets that are before the current month
    if (sheetYear < currentYear || (sheetYear === currentYear && sheetMonth < currentMonth)) {
      Logger.log(`Skipping sheet: ${sheetName} (before current month)`);
      return;
    }
    
    // Skip sheets that are after 2025-12
    if (sheetYear > 2025 || (sheetYear === 2025 && sheetMonth > 12)) {
      Logger.log(`Skipping sheet: ${sheetName} (after 2025-12)`);
      return;
    }
    
    Logger.log(`Processing sheet: ${sheetName}`);
    
    // Clear ALL existing protections first for this sheet
    const existingProtections = sheet.getProtections(SpreadsheetApp.ProtectionType.RANGE);
    for (let i = 0; i < existingProtections.length; i++) {
      Logger.log(`Removing existing protection: ${existingProtections[i].getDescription() || 'Unnamed protection'}`);
      existingProtections[i].remove();
    }
    
    // Get all formulas in the sheet
    const dataRange = sheet.getDataRange();
    const formulas = dataRange.getFormulas();
    
    // Create ranges for formula cells (processing by columns for efficiency)
    let protectionCount = 0;
    for (let col = 0; col < formulas[0].length; col++) {
      let startRow = null;
      
      for (let row = 0; row < formulas.length; row++) {
        if (formulas[row][col] !== "") {
          if (startRow === null) startRow = row;
        } else {
          if (startRow !== null) {
            // Protect this range of formulas
            const rangeToProtect = sheet.getRange(startRow + 1, col + 1, row - startRow, 1);
            protectRangeWithEditors(rangeToProtect, allowedEditors);
            protectionCount++;
            startRow = null;
          }
        }
      }
      
      // Handle case where formulas go to the last row
      if (startRow !== null) {
        const rangeToProtect = sheet.getRange(startRow + 1, col + 1, formulas.length - startRow, 1);
        protectRangeWithEditors(rangeToProtect, allowedEditors);
        protectionCount++;
      }
    }
    
    Logger.log(`Created ${protectionCount} protections for ${sheetName}`);
  });
  
  SpreadsheetApp.flush();
  Logger.log("Protection process complete");
}

// Helper function to protect a range and add specific editors
function protectRangeWithEditors(range, allowedEditors) {
  const protection = range.protect();
  const me = Session.getEffectiveUser();
  
  // First make sure you (the script runner) are an editor
  protection.addEditor(me);
  
  // Remove all other editors
  protection.removeEditors(protection.getEditors());
  
  // Add yourself back
  protection.addEditor(me);
  
  // Add all allowed editors
  allowedEditors.forEach(email => {
    if (email && email.includes('@')) {
      protection.addEditor(email);
    }
  });
  
  protection.setDescription("Protected formula cells");
}