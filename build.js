#!/usr/bin/env node
/**
 * TexTrack Build Script
 * Minifies inline JS in HTML files to obscure source code.
 * 
 * Setup (one time):
 *   npm install terser html-minifier-terser
 * 
 * Run:
 *   node build.js
 * 
 * Output: dist/ folder — upload these files to GitHub instead of the originals.
 */

const fs = require('fs');
const path = require('path');
const { minify: minifyJS } = require('terser');
const { minify: minifyHTML } = require('html-minifier-terser');

const FILES = ['control.html', 'device-v1.html', 'device-v2.html', 'admin.html', 'recovery-live.html'];
const DIST = path.join(__dirname, 'dist');

if (!fs.existsSync(DIST)) fs.mkdirSync(DIST);

const TERSER_OPTS = {
  compress: {
    dead_code: true,
    drop_console: true,      // removes all console.log/warn/error
    drop_debugger: true,
    passes: 2,
  },
  mangle: {
    toplevel: false,         // don't mangle top-level names (breaks some HTML onclick refs)
    reserved: [              // keep these names intact — referenced in HTML onclick attrs
      'doLogin', 'loadAll', 'showSection', 'closeModal', 'openAddInstaller',
      'addInstaller', 'showQrLink', 'generatePaymentLink', 'sendReactivationLink',
      'cycleMapLayer', 'cycleOrientation', 'fitBothOnMap', 'openGoogleMaps',
      'toggleVoice', 'handleRecoveryTap', 'loadHistory', 'clearHistory',
      'exportCSV', 'submitTransfer', 'changePinStep', 'sendCmd',
      'map', // leaflet global
    ]
  },
  format: {
    comments: false,         // strip all comments
    beautify: false,
  }
};

const HTML_OPTS = {
  collapseWhitespace: true,
  removeComments: true,
  removeRedundantAttributes: true,
  removeScriptTypeAttributes: true,
  removeStyleLinkTypeAttributes: true,
  minifyCSS: true,
  minifyJS: false, // we handle JS ourselves with terser
};

async function processFile(filename) {
  const src = fs.readFileSync(path.join(__dirname, filename), 'utf8');

  // Extract and minify each <script> block separately
  let result = src;
  const scriptRegex = /<script(?![^>]*src=)([^>]*)>([\s\S]*?)<\/script>/gi;
  const replacements = [];

  let match;
  while ((match = scriptRegex.exec(src)) !== null) {
    const attrs = match[1];
    const code = match[2].trim();
    if (!code) continue;

    try {
      const minified = await minifyJS(code, TERSER_OPTS);
      replacements.push({
        original: match[0],
        replacement: `<script${attrs}>${minified.code}</script>`
      });
    } catch(e) {
      console.warn(`  ⚠ JS minify error in ${filename}: ${e.message.slice(0, 80)}`);
      // Keep original on error
      replacements.push({ original: match[0], replacement: match[0] });
    }
  }

  // Apply JS replacements
  for (const { original, replacement } of replacements) {
    result = result.replace(original, replacement);
  }

  // Minify HTML (collapses whitespace, strips HTML comments)
  result = await minifyHTML(result, HTML_OPTS);

  const outPath = path.join(DIST, filename);
  fs.writeFileSync(outPath, result);

  const origSize = Buffer.byteLength(src, 'utf8');
  const newSize = Buffer.byteLength(result, 'utf8');
  const pct = Math.round((1 - newSize / origSize) * 100);
  console.log(`  ✓ ${filename}: ${(origSize/1024).toFixed(1)}KB → ${(newSize/1024).toFixed(1)}KB (-${pct}%)`);
}

async function build() {
  console.log('\nTexTrack Build\n' + '─'.repeat(40));
  for (const f of FILES) {
    if (fs.existsSync(path.join(__dirname, f))) {
      await processFile(f);
    } else {
      console.log(`  ⚠ Skipping ${f} (not found)`);
    }
  }
  console.log('\n✓ Done — upload files from dist/ to GitHub\n');
}

build().catch(e => { console.error('Build failed:', e.message); process.exit(1); });
