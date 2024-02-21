document.addEventListener("DOMContentLoaded", () => {
  const sliderContainer = document.getElementById("slider-container");

  if (!sliderContainer) {
    return;
  }

  const tabsContainer = document.getElementById("email-provider-tabs");
  if (tabsContainer) {
    const tabButtons = tabsContainer.children;
    for (let i = 0; i < tabButtons.length; i++) {
      tabButtons[i].addEventListener("click", () => {
        setActiveClass(tabButtons, i);
        moveSliderToIndex(sliderContainer, i);
      });
    }
  } else {
    const form = document.getElementById("email-factor");
    let mainFormAction = form.action;

    const hiddenInputs = Array.from(
      form.querySelectorAll("input[type=hidden]")
    ).filter((input) => !!input.dataset.secondaryValue);
    const hiddenInputValues = hiddenInputs.map((input) => input.value);

    if (!sliderContainer.children[0].classList.contains("active")) {
      form.action = form.dataset.secondaryAction;
      setInputValues(hiddenInputs);
    }

    document
      .getElementById("show-password-form")
      .addEventListener("click", () => {
        moveSliderToIndex(sliderContainer, 1);

        form.action = form.dataset.secondaryAction;
        setInputValues(hiddenInputs);

        document.getElementById("password")?.focus({ preventScroll: true });
      });

    document
      .getElementById("hide-password-form")
      .addEventListener("click", () => {
        moveSliderToIndex(sliderContainer, 0);

        form.action = mainFormAction;
        setInputValues(hiddenInputs, hiddenInputValues);

        failureRedirectInput.value = failureRedirect;
      });
  }
});

document.addEventListener("DOMContentLoaded", () => {
  const forgotLink = document.getElementById("forgot-password-link");
  const emailInput = document.getElementById("email");
  if (forgotLink) {
    const href = forgotLink.href;
    emailInput.addEventListener("input", (e) => {
      {
        forgotLink.href = `${href}&email=${encodeURIComponent(e.target.value)}`;
      }
    });
    forgotLink.href = `${href}&email=${encodeURIComponent(emailInput.value)}`;
  }

  const emailInputs = document.querySelectorAll("input[name=email]");
  for (const input of emailInputs) {
    input.addEventListener("input", (e) => {
      const val = e.target.value;
      for (const _input of emailInputs) {
        if (_input !== input) {
          _input.value = val;
        }
      }
    });
  }
});

/**
 * @param {HTMLElement[]} inputs
 * @param {string[]} [values]
 */
function setInputValues(inputs, values) {
  for (let i = 0; i < inputs.length; i++) {
    const input = inputs[i];
    input.value = values ? values[i] : input.dataset.secondaryValue;
  }
}

let firstInteraction = true;

/**
 * @param {HTMLElement} sliderContainer
 * @param {number} index
 */
function moveSliderToIndex(sliderContainer, index) {
  if (firstInteraction) {
    firstInteraction = false;
    // Fix the height of the main form card wrapper so the layout doesn't shift
    // when tabs are clicked
    const containerWrapper = document.getElementById("container-wrapper");
    containerWrapper.style.height =
      containerWrapper.getElementsByClassName("container")[0].clientHeight +
      "px";

    // Set the height for the first time as transition from 'auto' doesn't work
    sliderContainer.style.height = `${
      sliderContainer.getElementsByClassName("active")[0].scrollHeight
    }px`;
  }

  setActiveClass(sliderContainer.children, index);
  sliderContainer.style.transform = `translateX(${-100 * index}%)`;
  sliderContainer.style.height = `${sliderContainer.children[index].scrollHeight}px`;
}

/**
 * @param {HTMLCollection} items
 * @param {number} index
 */
function setActiveClass(items, index) {
  for (let i = 0; i < items.length; i++) {
    if (i === index) {
      items[i].classList.add("active");
    } else {
      items[i].classList.remove("active");
    }
  }
}
