// ===== Deployment Script with Cross‑Spreadsheet Formatting Copy =====
// (Google Apps Script)
// 2025‑05‑08 — fixes "Zieltabellenblatt und Quellbereich müssen sich in derselben Tabelle befinden" 
//             and TypeError: setHidden → hideSheet()
// by copying the template sheet into the *target* file temporarily, so all
// formatting is applied inside the same spreadsheet.

/**
 * CONFIG — central place for IDs, sheet names and preserved ranges
 */
const CONFIG = {
  // ▸ ID of the spreadsheet that holds your templates
  TEMPLATE_ID: '1OgE3WZcgOeQBaFV1P3yKyouREHQUhlm3rh3CYm1MQaA',

  // ▸ Names of the template sheets inside TEMPLATE_ID
  TEMPLATE_SHEETS: {
    MONTH: 'template_month',          // monthly planning sheet
    MONITORING: 'Project Monitoring'  // team-level monitoring sheet
  },

  // ▸ Ranges whose *values* you want to preserve every month
  MONTHLY_PRESERVED_RANGES: [
    { range: 'H39:H63', description: 'Custom Range 1' },
    { range: 'J73:M97', description: 'Custom Range 2' }
  ],

  // ▸ Ranges whose *values* you want to preserve everywhere
  GLOBAL_PRESERVED_RANGES: [
    { range: 'E5', description: 'Team Name' },
    { range: 'F5', description: 'Department' },
    { range: 'C5', description: 'Planning Month' },
    { range: 'G5', description: 'Avg Sick Leave Reference Month' },
    { range: 'B5', description: 'Teamlead' }
  ],

  // ▸ All team spreadsheets that should receive the update
  TARGET_SHEETS: [
    {
      id: '1gAeqo3B0D-w9OiysvsG8oYf1FZnbhmcmF6AbPP7S8oM',
      name: 'Kappa Planung – Strang PA',
      department: 'Paid Media',
      team: 'PA'
    },
    {
      id: '1WSNHyq_0wD_gJ76SqT_f931DujYuFiHumOnuHoBvteQ',
      name: 'Kappa Planung – Strang PB',
      department: 'Paid Media',
      team: 'PB'
    },
    {
      id: '1h2_bdHXFYAspkwBNJKs_RqnNoE9emi3hwXYYPAjtr4M',
      name: 'Kappa Planung – Strang PC',
      department: 'Paid Media',
      team: 'PC'
    },
    {
      id: '1VjfY8f7FzpmuRHNXGcbdNQfm6ZoqLljQmTTaAFHAdCU',
      name: 'Kappa Planung – Strang PD',
      department: 'Paid Media',
      team: 'PD'
    },
    {
      id: '1U954MuWUbraUQU0cVKjqrq7Dhl1Sh_lqzcLZAk74pog',
      name: 'Kappa Planung – Strang PE',
      department: 'Paid Media',
      team: 'PE'
    }
  ]
};

// ===== Helper utilities =====

/** Regex-test for YYYY-MM sheet names */
function isMonthSheet(sheetName) {
  return /^\d{4}-\d{2}$/.test(sheetName);
}

/** Pull values from an array of ranges */
function getPreservedValues(sheet, ranges) {
  const out = {};
  ranges.forEach(r => out[r.range] = sheet.getRange(r.range).getValues());
  return out;
}

/** Push values back into the same A1 ranges */
function restorePreservedValues(sheet, preserved) {
  Object.entries(preserved).forEach(([rng, vals]) => sheet.getRange(rng).setValues(vals));
}

/** Column number → letter helper (A, B, … AA) */
function columnToLetter(col) {
  let s = '';
  while (col > 0) {
    const t = (col - 1) % 26;
    s = String.fromCharCode(65 + t) + s;
    col = (col - t - 1) / 26;
  }
  return s;
}

/**
 * Copy template formatting (backgrounds, fonts, borders, conditional rules)
 * across spreadsheets ― works around the copyFormatToRange limitation by
 * temporarily copying the *whole* template sheet into the destination file.
 * That keeps both source & target ranges in the same spreadsheet.
 */
function copyTemplateFormatting(templateSheet, targetSheet) {
  // 1) temp‑clone of the template inside the *same* spreadsheet as the target
  const targetSS = targetSheet.getParent();
  const temp     = templateSheet.copyTo(targetSS);
  temp.hideSheet();

  // 2) transfer ordinary formats (skip row 5 that you keep as‑is)
  temp.getRange('A1:Z4').copyFormatToRange(targetSheet, 1, 26, 1, 4);
  temp.getRange('A6:Z1000').copyFormatToRange(targetSheet, 1, 26, 6, 1000);

  // 3) rebuild conditional‑formatting rules so ranges point to *this* sheet
  try {
    const newRules = temp.getConditionalFormatRules().map(rule => {
      const newRanges = rule.getRanges().map(r =>
        targetSheet.getRange(r.getRow(), r.getColumn(), r.getNumRows(), r.getNumColumns())
      );
      return rule.copy().setRanges(newRanges).build();
    });
    targetSheet.setConditionalFormatRules(newRules);
  } catch (err) {
    Logger.log('Überspringe bedingte Formatierungen: ' + err);
  }

  // 4) delete helper sheet
  targetSS.deleteSheet(temp);
}

// ===== Core sheet-update functions =====

function updateMonthlySheet(templateSheet, targetSheet) {
  const blankRanges = ['B15:B30', 'B40:B63', 'B74:B97', 'C102:D102', 'B103:D111', 'B127:B142', 'B153:B169'];
  const preserved   = getPreservedValues(targetSheet, CONFIG.MONTHLY_PRESERVED_RANGES);

  // --- merge template formulas & values (skip row 5) ---
  const tplRange = templateSheet.getRange('A1:Z1000');
  const tplVals  = tplRange.getValues();
  const tplForms = tplRange.getFormulas();

  const merged = tplVals.map((row, r) => row.map((val, c) => {
    if (r === 4) return targetSheet.getRange(r + 1, c + 1).getValue();
    return tplForms[r][c] || val;
  }));

  targetSheet.getRange('A1:Z1000').setValues(merged);

  // --- now copy *just* the look & feel ---
  copyTemplateFormatting(templateSheet, targetSheet);

  blankRanges.forEach(rng => targetSheet.getRange(rng).clearContent());
  restorePreservedValues(targetSheet, preserved);
}

function updateMonitoringSheet(templateSheet, targetSheet) {
  const blankRanges = ['B16:B39', 'B50:B73', 'C80:D80', 'B81:D89', 'B103:B111'];
  const preserved   = getPreservedValues(targetSheet, CONFIG.GLOBAL_PRESERVED_RANGES);

  const tplRange = templateSheet.getRange('A1:Z1000');
  const tplVals  = tplRange.getValues();
  const tplForms = tplRange.getFormulas();

  const merged = tplVals.map((row, r) => row.map((val, c) => {
    if (r === 4) return targetSheet.getRange(r + 1, c + 1).getValue();
    return tplForms[r][c] || val;
  }));

  targetSheet.getRange('A1:Z1000').setValues(merged);
  copyTemplateFormatting(templateSheet, targetSheet);

  blankRanges.forEach(rng => targetSheet.getRange(rng).clearContent());
  restorePreservedValues(targetSheet, preserved);
}

// ===== Deployment tasks =====

function deployToTeamSheets() {
  const tplSS      = SpreadsheetApp.openById(CONFIG.TEMPLATE_ID);
  const monthTpl   = tplSS.getSheetByName(CONFIG.TEMPLATE_SHEETS.MONTH);
  const monitorTpl = tplSS.getSheetByName(CONFIG.TEMPLATE_SHEETS.MONITORING);
  if (!monthTpl || !monitorTpl) throw new Error('Template sheets missing');

  CONFIG.TARGET_SHEETS.forEach(tgt => {
    const ss = SpreadsheetApp.openById(tgt.id);
    ss.getSheets().forEach(sh => {
      const n = sh.getName();
      if (isMonthSheet(n))                        updateMonthlySheet(monthTpl,  sh);
      if (n === CONFIG.TEMPLATE_SHEETS.MONITORING) updateMonitoringSheet(monitorTpl, sh);
    });
  });
}

function deployNotesToTeamSheets() {
  const devSS = SpreadsheetApp.getActiveSpreadsheet();
  const notes = new Map();

  devSS.getSheets().forEach(sh => {
    if (sh.getName().startsWith('Aggregated_')) return;
    const lr = sh.getLastRow();
    const lc = sh.getLastColumn();
    if (!lr || !lc) return;

    const ns = sh.getRange(1, 1, lr, lc).getNotes();
    for (let r = 0; r < ns.length; r++) {
      for (let c = 0; c < ns[r].length; c++) {
        if (ns[r][c]) notes.set(`${sh.getName()}!${sh.getRange(r + 1, c + 1).getA1Notation()}`, ns[r][c]);
      }
    }
  });

  if (notes.size === 0) {
    SpreadsheetApp.getUi().alert('Keine Notizen gefunden.');
    return;
  }

  CONFIG.TARGET_SHEETS.forEach(tgt => {
    const ss = SpreadsheetApp.openById(tgt.id);
    notes.forEach((note, loc) => {
      const [shName, a1] = loc.split('!');
      const sh = ss.getSheetByName(shName);
      if (sh) sh.getRange(a1).setNote(note);
    });
  });

  SpreadsheetApp.getUi().alert(`Es wurden ${notes.size} Notizen auf ${CONFIG.TARGET_SHEETS.length} Dateien übertragen.`);
}

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Deployment')
    .addItem('Templates auf alle Teams verteilen', 'deployToTeamSheets')
    .addItem('Notizen verteilen', 'deployNotesToTeamSheets')
    .addToUi();
}

// ===== End of file =====
