import React from 'react';

interface ButtonProps extends React.ButtonHTMLAttributes<HTMLButtonElement> {
  variant?: 'primary' | 'ghost' | 'outline' | 'compact' | 'danger';
  size?: 'sm' | 'md' | 'lg';
  loading?: boolean;
  iconLeft?: React.ReactNode;
  iconRight?: React.ReactNode;
}

export const Button = React.forwardRef<HTMLButtonElement, ButtonProps>(
  (
    {
      variant = 'primary',
      size = 'md',
      loading = false,
      iconLeft,
      iconRight,
      className = '',
      disabled,
      children,
      ...props
    },
    ref
  ) => {
    const variantStyles = {
      primary: 'bg-primary text-on-primary hover:bg-cohere-black transition-colors',
      ghost: 'bg-transparent border border-hairline text-ink hover:bg-soft-stone border-hairline',
      outline: 'bg-transparent border border-hairline text-ink hover:bg-soft-stone border-slate',
      compact: 'bg-canvas border border-hairline text-ink hover:bg-primary hover:text-white border-primary',
      danger: 'bg-error text-white hover:bg-b30000',
    };

    const sizeStyles = {
      sm: 'px-3 py-1.5 text-xs',
      md: 'px-4 py-2 text-sm',
      lg: 'px-6 py-3 text-base',
    };

    const baseClass = 'inline-flex items-center justify-center gap-2 font-sans font-medium transition-all duration-150 focus:outline-none focus:ring-2 focus:ring-offset-2 focus:ring-primary disabled:opacity-50 disabled:cursor-not-allowed';

    const fullClassName = `
      ${baseClass}
      ${variantStyles[variant]}
      ${sizeStyles[size]}
      ${className}
    `.trim();

    return (
      <button
        ref={ref}
        className={fullClassName}
        disabled={disabled || loading}
        {...props}
      >
        {loading ? (
          <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24">
            <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" fill="none" />
            <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
          </svg>
        ) : (
          <>
            {iconLeft && <span className="flex-shrink-0">{iconLeft}</span>}
            {children}
            {iconRight && <span className="flex-shrink-0">{iconRight}</span>}
          </>
        )}
      </button>
    );
  }
);

Button.displayName = 'Button';
