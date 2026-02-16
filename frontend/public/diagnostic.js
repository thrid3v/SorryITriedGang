// Simple test to check if React app loads
console.log('=== REACT APP DIAGNOSTIC ===');
console.log('1. React loaded:', typeof React !== 'undefined');
console.log('2. ReactDOM loaded:', typeof ReactDOM !== 'undefined');
console.log('3. Window location:', window.location.href);
console.log('4. LocalStorage auth:', localStorage.getItem('isAuthenticated'));
console.log('5. LocalStorage user:', localStorage.getItem('user'));

// Test API fetch
fetch('/api/kpis')
  .then(r => {
    console.log('6. API /kpis status:', r.status);
    return r.json();
  })
  .then(data => console.log('7. API /kpis data:', data))
  .catch(err => console.error('8. API /kpis error:', err));

console.log('=== END DIAGNOSTIC ===');
