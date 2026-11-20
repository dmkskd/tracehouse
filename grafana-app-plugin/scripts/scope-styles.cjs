const selectorParser = require('postcss-selector-parser');

const scope = ':where(:local(.root))';
const excluded = ':not(:where([data-tracehouse-grafana], [data-tracehouse-grafana] *))';
const namespace = 'dmkskd-tracehouse-app';
const parseSelector = (text) => selectorParser().astSync(text).first.nodes;

function inKeyframes(rule) {
  for (let parent = rule.parent; parent; parent = parent.parent) {
    if (parent.type === 'atrule' && /keyframes$/.test(parent.name)) return true;
  }
  return false;
}

/** Scope generated/shared CSS; preserve explicit source scopes and cascade layers. */
module.exports = () => ({
  postcssPlugin: 'tracehouse-style-scope',
  OnceExit(root) {
    const animations = new Map();
    root.walkAtRules(/keyframes$/, (rule) => {
      const original = rule.params;
      const name = `${namespace}-${original}`;
      animations.set(original, name);
      rule.params = name;
    });
    // @property registrations have document-wide names even when selectors are scoped.
    // Namespace Tailwind's internal variables, including its fallback declarations.
    const internalVariable = /--tw-[\w-]+/g;
    const renameVariables = (value) =>
      value.replace(internalVariable, (name) => `--${namespace}-${name.slice(2)}`);
    root.walkDecls((declaration) => {
      declaration.prop = renameVariables(declaration.prop);
      declaration.value = renameVariables(declaration.value);
      if (/animation|^--animate-/.test(declaration.prop)) {
        declaration.value = declaration.value.replace(/[\w-]+/g, (name) => animations.get(name) ?? name);
      }
    });
    root.walkAtRules((rule) => {
      if (rule.name === 'property') rule.params = renameVariables(rule.params);
      if (rule.name === 'layer') {
        rule.params = rule.params.replace(/[\w-]+/g, (name) => `${namespace}-${name}`);
      }
    });
    root.walkRules((rule) => {
      if (inKeyframes(rule)) return;
      const nested =
        rule.parent.type === 'rule' ||
        (() => {
          for (let parent = rule.parent; parent; parent = parent.parent) {
            if (parent.type === 'rule') return true;
          }
          return false;
        })();
      rule.selector = selectorParser((selectors) => {
        selectors.each((selector) => {
          const first = selector.first;
          const rootSelector =
            first &&
            ((first.type === 'pseudo' && [':root', ':host'].includes(first.value)) ||
              (first.type === 'tag' && ['html', 'body'].includes(first.value)) ||
              (first.type === 'class' && first.value === 'grafana-app-container'));
          const themed =
            first &&
            ((first.type === 'attribute' && first.attribute === 'data-theme') ||
              (first.type === 'class' && ['theme-light', 'theme-dark'].includes(first.value)));
          // Handwritten plugin CSS already names the local root. Tailwind and
          // shared standalone styles still need their scope added here.
          const explicitlyScoped = first?.toString().trim() === scope;
          if (!nested && !explicitlyScoped) {
            const nodes = parseSelector(scope).map((node) => node.clone());
            if (rootSelector) first.replaceWith(...nodes);
            else if (themed) {
              first.spaces.before = '';
              selector.prepend(...nodes);
            } else {
              // Leading list whitespace must not separate an exclusion from
              // its pseudo-element (for example ", ::before").
              first.spaces.before = '';
              selector.prepend(selectorParser.combinator({ value: ' ' }));
              selector.prepend(...nodes);
            }
          }
          // Exclude the marked Grafana element itself as well as all descendants.
          // Insert before pseudo-elements, where :not() is valid.
          let subject = selector.last;
          while (subject && subject.type !== 'combinator') {
            if (subject.type === 'pseudo' && subject.value.startsWith('::')) break;
            subject = subject.prev();
          }
          const exclusion = parseSelector(excluded).map((node) => node.clone());
          if (subject?.type === 'pseudo') selector.insertBefore(subject, exclusion[0]);
          else selector.append(...exclusion);
        });
      }).processSync(rule.selector);
    });
  },
});
module.exports.postcss = true;
