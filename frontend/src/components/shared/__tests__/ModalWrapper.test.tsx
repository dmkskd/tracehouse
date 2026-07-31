import { fireEvent, render, screen } from '@testing-library/react';
import { describe, expect, it, vi } from 'vitest';
import { ModalWrapper } from '../ModalWrapper';

describe('ModalWrapper', () => {
  it('portals the dialog to the document body so it escapes parent stacking contexts', () => {
    const parent = document.createElement('div');
    document.body.appendChild(parent);

    const { unmount } = render(
      <div data-testid="stacking-context" style={{ position: 'relative', zIndex: 1 }}>
        <ModalWrapper isOpen onClose={vi.fn()} ariaLabel="Example modal">
          Modal content
        </ModalWrapper>
      </div>,
      { container: parent },
    );

    const dialog = screen.getByRole('dialog', { name: 'Example modal' });
    expect(dialog.parentElement).toBe(document.body);
    expect(screen.getByTestId('stacking-context')).not.toContainElement(dialog);

    unmount();
    parent.remove();
  });

  it('closes when its backdrop is clicked', () => {
    const onClose = vi.fn();

    render(
      <ModalWrapper isOpen onClose={onClose} ariaLabel="Closable modal">
        Modal content
      </ModalWrapper>,
    );

    const dialog = screen.getByRole('dialog', { name: 'Closable modal' });
    fireEvent.click(dialog.firstElementChild as Element);

    expect(onClose).toHaveBeenCalledOnce();
  });
});
