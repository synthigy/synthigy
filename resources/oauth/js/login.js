// Synthigy Login

(function() {
  var userInput = document.getElementById('username');
  var passwordInput = document.getElementById('password');

  function setupRow(input) {
    if (!input) return;
    var row = input.closest('.row');
    if (!row) return;

    function updateActive() {
      if (input.value !== '' || document.activeElement === input) {
        row.classList.add('active');
      } else {
        row.classList.remove('active');
      }
    }

    input.addEventListener('focus', updateActive);
    input.addEventListener('blur', updateActive);
    input.addEventListener('input', updateActive);

    updateActive();
  }

  setupRow(userInput);
  setupRow(passwordInput);

  if (userInput) {
    userInput.focus();
  }
})();
