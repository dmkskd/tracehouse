import { useState } from 'react';
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
  it('keeps keyboard focus in the top dialog and excludes hidden controls', () => {
    render(<ModalWrapper isOpen onClose={vi.fn()} ariaLabel="Focus scope"><button>First</button><div hidden><button>Hidden</button></div><button>Last</button></ModalWrapper>);
    const first = screen.getByRole('button', { name: 'First' }), last = screen.getByRole('button', { name: 'Last' });
    for (const el of [first, last]) vi.spyOn(el, 'getClientRects').mockReturnValue([{}] as unknown as DOMRectList);
    last.focus(); fireEvent.keyDown(document, { key: 'Tab' });
    expect(first).toHaveFocus();
    fireEvent.keyDown(document, { key: 'Tab', shiftKey: true });
    expect(last).toHaveFocus();
  });
  it('closes only the top modal and restores its parent focus and scroll lock', () => {
    const parentClose = vi.fn(), childClose = vi.fn();
    function Nested() {
      const [child, setChild] = useState(false);
      return <>
        <ModalWrapper isOpen onClose={() => parentClose()} ariaLabel="Resource flow">
          <button onClick={() => setChild(true)}>Open query</button>
        </ModalWrapper>
        {child && <ModalWrapper isOpen onClose={() => { childClose(); setChild(false); }} ariaLabel="Query details"><button>Close query</button></ModalWrapper>}
      </>;
    }
    render(<Nested />);
    const trigger = screen.getByRole('button', { name: 'Open query' });
    trigger.focus(); fireEvent.click(trigger);
    expect(screen.getAllByRole('dialog')).toHaveLength(2);
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(childClose).toHaveBeenCalledOnce();
    expect(parentClose).not.toHaveBeenCalled();
    expect(screen.getByRole('dialog', { name: 'Resource flow' })).toBeInTheDocument();
    expect(document.body.style.overflow).toBe('hidden');
    expect(trigger).toHaveFocus();
    fireEvent.keyDown(document, { key: 'Escape' });
    expect(parentClose).toHaveBeenCalledOnce();
  });

});
