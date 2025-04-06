package velum

type TableConfig struct {
	tag            string
	name           string
	argFormatter   ArgFormatter
	colNameBuilder func(attr, tag string) string
	seqNameBuilder func(string) string
}

type TableOption func(*TableConfig)

func WithTag(tag string) TableOption {
	return func(o *TableConfig) {
		o.tag = tag
	}
}

func WithName(name string) TableOption {
	return func(o *TableConfig) {
		o.name = name
	}
}

func WithArgFormatter(f ArgFormatter) TableOption {
	return func(o *TableConfig) {
		o.argFormatter = f
	}
}

func WithColumnNameBuilder(f func(attr string, tag string) string) TableOption {
	return func(o *TableConfig) {
		o.colNameBuilder = f
	}
}

func WithSequenceNameBuilder(f func(string) string) TableOption {
	return func(o *TableConfig) {
		o.seqNameBuilder = f
	}
}
