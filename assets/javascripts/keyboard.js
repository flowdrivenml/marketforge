// Allow keyboard focus on the responsive controls in Zensical 0.1.1.
// The theme already handles keyboard activation of its checkbox labels.
for (const control of document.querySelectorAll(
  '.md-header label[for="__drawer"], .md-header label[for="__search"]'
)) {
  control.tabIndex = 0;
  control.setAttribute("role", "button");
}
