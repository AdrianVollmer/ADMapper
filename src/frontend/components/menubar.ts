/**
 * Menu Bar Component
 *
 * Handles classic dropdown menu behavior with keyboard navigation.
 */

import { dispatchAction, updateRecentConnectionsMenu, type Action } from "./actions";

let activeMenu: HTMLElement | null = null;

/** Initialize the menu bar */
export function initMenuBar(): void {
  const menubar = document.getElementById("menubar");
  if (!menubar) return;

  // Handle menu trigger clicks
  menubar.addEventListener("click", (e) => {
    const trigger = (e.target as HTMLElement).closest(".menu-trigger");
    if (trigger) {
      e.stopPropagation();
      const menuItem = trigger.closest(".menu-item") as HTMLElement;
      toggleMenu(menuItem);
      return;
    }

    // Handle menu option clicks
    const option = (e.target as HTMLElement).closest(".menu-option");
    if (option) {
      // Don't dispatch if disabled
      if (option.hasAttribute("disabled") || option.classList.contains("menu-disabled")) {
        return;
      }
      const action = option.getAttribute("data-action");
      if (action) {
        closeAllMenus();
        dispatchAction(action as Action);
      }
    }
  });

  // Handle hover to switch menus when one is open
  menubar.addEventListener("mouseover", (e) => {
    if (!activeMenu) return;

    const menuItem = (e.target as HTMLElement).closest(".menu-item") as HTMLElement;
    if (menuItem && menuItem !== activeMenu) {
      closeAllMenus();
      openMenu(menuItem);
    }
  });

  // Keyboard navigation
  menubar.addEventListener("keydown", handleMenuKeydown);
}

/**
 * Handle clicks outside the menubar to close open menus.
 * Called from the central document click handler in main.ts.
 * This doesn't return a value since it should always allow other handlers to run.
 */
export function handleMenubarOutsideClick(e: MouseEvent): void {
  const menubar = document.getElementById("menubar");
  if (activeMenu && menubar && !menubar.contains(e.target as Node)) {
    closeAllMenus();
  }
}

/** Toggle a menu open/closed */
function toggleMenu(menuItem: HTMLElement): void {
  if (activeMenu === menuItem) {
    closeAllMenus();
  } else {
    closeAllMenus();
    openMenu(menuItem);
  }
}

/** Open a menu */
function openMenu(menuItem: HTMLElement): void {
  const trigger = menuItem.querySelector(".menu-trigger");
  const dropdown = menuItem.querySelector(".menu-dropdown");

  if (trigger && dropdown) {
    trigger.setAttribute("aria-expanded", "true");
    dropdown.removeAttribute("hidden");
    activeMenu = menuItem;

    // Update recent connections when File menu is opened
    const menuName = menuItem.getAttribute("data-menu");
    if (menuName === "file") {
      updateRecentConnectionsMenu();
    }

    // Focus first option
    const firstOption = dropdown.querySelector(".menu-option") as HTMLElement;
    firstOption?.focus();
  }
}

/** Close all menus */
function closeAllMenus(): void {
  const triggers = document.querySelectorAll(".menu-trigger");
  const dropdowns = document.querySelectorAll(".menu-dropdown");

  for (const trigger of triggers) {
    trigger.setAttribute("aria-expanded", "false");
  }

  for (const dropdown of dropdowns) {
    dropdown.setAttribute("hidden", "");
  }

  activeMenu = null;
}

/** Handle keyboard navigation in menus */
function handleMenuKeydown(e: KeyboardEvent): void {
  if (!activeMenu) {
    // Open menu on Enter/Space/ArrowDown when trigger is focused
    if (e.key === "Enter" || e.key === " " || e.key === "ArrowDown") {
      const trigger = document.activeElement?.closest(".menu-item");
      if (trigger) {
        e.preventDefault();
        openMenu(trigger as HTMLElement);
      }
    }
    return;
  }

  const dropdown = activeMenu.querySelector(".menu-dropdown");
  if (!dropdown) return;

  const options = Array.from(dropdown.querySelectorAll(".menu-option"));
  const currentIndex = options.indexOf(document.activeElement as HTMLElement);

  switch (e.key) {
    case "ArrowDown":
      e.preventDefault();
      if (currentIndex < options.length - 1) {
        (options[currentIndex + 1] as HTMLElement).focus();
      }
      break;

    case "ArrowUp":
      e.preventDefault();
      if (currentIndex > 0) {
        (options[currentIndex - 1] as HTMLElement).focus();
      }
      break;

    case "ArrowLeft": {
      e.preventDefault();
      const menuItems = Array.from(document.querySelectorAll(".menu-item"));
      const currentMenuIndex = menuItems.indexOf(activeMenu);
      if (currentMenuIndex > 0) {
        closeAllMenus();
        openMenu(menuItems[currentMenuIndex - 1] as HTMLElement);
      }
      break;
    }

    case "ArrowRight": {
      e.preventDefault();
      const menuItems = Array.from(document.querySelectorAll(".menu-item"));
      const currentMenuIndex = menuItems.indexOf(activeMenu);
      if (currentMenuIndex < menuItems.length - 1) {
        closeAllMenus();
        openMenu(menuItems[currentMenuIndex + 1] as HTMLElement);
      }
      break;
    }

    case "Enter":
    case " ":
      e.preventDefault();
      if (document.activeElement instanceof HTMLElement) {
        document.activeElement.click();
      }
      break;

    case "Escape": {
      e.preventDefault();
      closeAllMenus();
      // Return focus to the menu trigger
      const trigger = activeMenu?.querySelector(".menu-trigger") as HTMLElement;
      trigger?.focus();
      break;
    }
  }
}
