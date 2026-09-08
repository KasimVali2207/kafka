// syntax.js – Lazy load & Interactive Controller for Kafka Syntax & Troubleshooting Cheat Sheet

(() => {
  const placeholder = document.getElementById('syntax-placeholder');
  const loadBtn = document.getElementById('load-syntax-btn');

  // Debounce helper
  const debounce = (func, wait) => {
    let timeout;
    return (...args) => {
      clearTimeout(timeout);
      timeout = setTimeout(() => func.apply(this, args), wait);
    };
  };

  // Fetch syntax.html and initialize
  const loadSyntax = async () => {
    try {
      const resp = await fetch('syntax.html');
      if (!resp.ok) throw new Error('Failed to fetch syntax.html');
      const html = await resp.text();
      placeholder.innerHTML = html;
      initCheatSheet();
    } catch (e) {
      console.error('Error loading syntax cheat sheet:', e);
      if (placeholder) {
        placeholder.innerHTML = '<div style="color:#FCA5A5; padding:1.5rem; text-align:center;">Failed to load cheat sheet. Please ensure syntax.html is in the project folder.</div>';
      }
    }
  };

  // Wire events, tabs, search, and copy buttons
  const initCheatSheet = () => {
    const tabs = document.querySelectorAll('.syntax-tab');
    const panels = document.querySelectorAll('.syntax-panel');
    const searchInput = document.getElementById('syntax-search');

    // 1. Tab Switching
    tabs.forEach(tab => {
      tab.addEventListener('click', () => {
        const target = tab.dataset.tab;
        tabs.forEach(t => t.classList.toggle('active', t === tab));
        panels.forEach(p => p.classList.toggle('active', p.dataset.panel === target));

        // Re-apply search filter to newly active panel
        if (searchInput && searchInput.value.trim()) {
          applyFilter(searchInput.value.trim().toLowerCase());
        }
      });
    });

    // 2. Clipboard Copy Button Handler
    document.querySelectorAll('.syn-copy-btn').forEach(btn => {
      btn.addEventListener('click', async (e) => {
        e.stopPropagation();
        const card = btn.closest('.syn-card');
        if (!card) return;
        const codeElem = card.querySelector('code');
        if (!codeElem) return;

        const textToCopy = codeElem.innerText;
        try {
          await navigator.clipboard.writeText(textToCopy);
          const origText = btn.innerHTML;
          btn.innerHTML = '✅ Copied!';
          btn.classList.add('copied');
          setTimeout(() => {
            btn.innerHTML = origText;
            btn.classList.remove('copied');
          }, 2000);
        } catch (err) {
          // Fallback if clipboard API restricted
          const ta = document.createElement('textarea');
          ta.value = textToCopy;
          ta.style.position = 'fixed';
          ta.style.opacity = '0';
          document.body.appendChild(ta);
          ta.select();
          document.execCommand('copy');
          document.body.removeChild(ta);
          btn.innerHTML = '✅ Copied!';
          btn.classList.add('copied');
          setTimeout(() => {
            btn.innerHTML = '📋 Copy';
            btn.classList.remove('copied');
          }, 2000);
        }
      });
    });

    // 3. Reactive Search Filter
    const applyFilter = (term) => {
      const activePanel = document.querySelector('.syntax-panel.active');
      if (!activePanel) return;

      const cards = activePanel.querySelectorAll('.syn-card, .syn-concept-card');
      cards.forEach(card => {
        if (!term) {
          card.classList.remove('search-hidden');
          return;
        }
        const text = (card.innerText + ' ' + (card.dataset.tags || '')).toLowerCase();
        const match = text.includes(term);
        card.classList.toggle('search-hidden', !match);
      });
    };

    if (searchInput) {
      searchInput.addEventListener('input', debounce(() => {
        applyFilter(searchInput.value.trim().toLowerCase());
      }, 150));
    }
  };

  // Bind trigger button
  if (loadBtn) {
    loadBtn.addEventListener('click', () => {
      loadBtn.disabled = true;
      loadBtn.textContent = 'Loading Cheat Sheet…';
      loadSyntax();
    });
  } else {
    // If placeholder is already present without a button, auto-load
    loadSyntax();
  }
})();
