# The awk half of the seeded argv-boundary fuzz (tests/run_argv_fuzz.sh).
# tests/helpers/argv_fuzz.sh runs it as
#   LC_ALL=C awk -v role=ROLE -f tests/helpers/argv_fuzz.awk -- [ARG...]
# with ROLE one of:
#   gen    write case ENVIRON["ARGV_FUZZ_CASE"] of seed ["ARGV_FUZZ_SEED"]
#          into directory ["ARGV_FUZZ_CASE_DIR"] (see gen_main)
#   zfs    act as zfs for the ARG list against the model in directory
#          ["ARGV_FUZZ_STATE"], logging the argv before anything else
#   check  print one problem per line for the finished run in
#          ["ARGV_FUZZ_STATE"] and exit 1 when there is any (see check_main)
# Everything runs in BEGIN, and LC_ALL=C keeps every string a byte string.
#
# Escaped text: printable ASCII stays as it is, a backslash becomes "\\" and
# any other byte "\ooo". The encoding is one-to-one and holds no tab or
# newline, so escaped fields are TAB-separated with one record per line.
#
# Model file ("model"; names and values escaped, TAB-separated). The fake zfs
# appends one record per mutation, and a later record wins:
#   D name guid                     a dataset exists
#   V name volsize                  that dataset is a volume (zvol)
#   M name                          a destination dataset zxfer may create
#   S dataset snap guid time        a snapshot exists
#   P dataset property value source a property row (source "local" or "-")
#   X dataset property              zfs inherit dropped a local row
# Awk compares strings that look numeric as numbers, so names, values and
# guids are compared with same(), which forces a string comparison.

BEGIN {
	for (code = 1; code < 256; code++) {
		byte = sprintf("%c", code)
		byte_code[byte] = code
		octal_byte[sprintf("%03o", code)] = byte
	}
	status = 0
	if (role == "gen")
		gen_main()
	else if (role == "zfs")
		status = zfs_main()
	else if (role == "check")
		status = check_main()
	else {
		print "argv_fuzz.awk: unknown role: " role > "/dev/stderr"
		status = 2
	}
	exit status
}

################################################################################
# Shared: escaping, the model, and property inheritance.

function esc(text,    out, i, n, c, code) {
	out = ""
	n = length(text)
	for (i = 1; i <= n; i++) {
		c = substr(text, i, 1)
		code = byte_code[c]
		if (c == "\\")
			out = out "\\\\"
		else if (code >= 32 && code <= 126)
			out = out c
		else
			out = out "\\" sprintf("%03o", code)
	}
	return out
}

function unesc(text,    out, at) {
	out = ""
	while ((at = index(text, "\\")) > 0) {
		out = out substr(text, 1, at - 1)
		if (substr(text, at + 1, 1) == "\\") {
			out = out "\\"
			text = substr(text, at + 2)
		} else {
			out = out octal_byte[substr(text, at + 1, 3)]
			text = substr(text, at + 4)
		}
	}
	return out text
}

function same(left, right) {
	return (left "") == (right "")
}

function parent_of(name,    parent) {
	parent = name
	sub(/\/[^\/]*$/, "", parent)
	return same(parent, name) ? "" : parent
}

function load_model(file,    line, f, name, key) {
	while ((getline line < file) > 0) {
		split(line, f, "\t")
		name = unesc(f[2])
		if (f[1] == "D") {
			ds_guid[name] = f[3]
		} else if (f[1] == "V") {
			ds_volsize[name] = f[3]
		} else if (f[1] == "M") {
			creatable[name] = 1
		} else if (f[1] == "S") {
			key = name "@" unesc(f[3])
			if (!(key in snap_guid)) {
				snap_at[name, ++snap_count[name]] = unesc(f[3])
				snap_names[unesc(f[3])] = 1
			}
			snap_guid[key] = f[4]
			snap_time[key] = f[5] + 0
		} else if (f[1] == "P") {
			if (!((name, f[3]) in prop_listed)) {
				prop_listed[name, f[3]] = 1
				prop_at[name, ++prop_count[name]] = f[3]
			}
			prop_value[name, f[3]] = unesc(f[4])
			prop_source[name, f[3]] = f[5]
			known_property[f[3]] = 1
		} else if (f[1] == "X") {
			delete prop_value[name, f[3]]
			delete prop_source[name, f[3]]
		}
	}
	close(file)
}

# Sets eff_value to the value of PROPERTY on DATASET: its own row, else the
# local row of the nearest ancestor. Returns 0 when neither exists.
function effective(dataset, property,    ancestor) {
	if ((dataset, property) in prop_source) {
		eff_value = prop_value[dataset, property]
		return 1
	}
	for (ancestor = parent_of(dataset); ancestor != ""; ancestor = parent_of(ancestor)) {
		if (((ancestor, property) in prop_source) && prop_source[ancestor, property] == "local") {
			eff_value = prop_value[ancestor, property]
			return 1
		}
	}
	return 0
}

function dataset_type(name) {
	return (name in ds_volsize) ? "volume" : "filesystem"
}

# Fills row_name/row_value/row_source[1..row_count] with the `zfs get all`
# rows of one dataset: type, its own rows, rows inherited from ancestors, and
# then the volume size and block size of a volume, or the creation-time
# properties every filesystem carries.
function collect_properties(dataset,    seen, k, property, ancestor) {
	row_count = 0
	add_row("type", dataset_type(dataset), "-")
	for (k = 1; k <= prop_count[dataset]; k++) {
		property = prop_at[dataset, k]
		if ((dataset, property) in prop_source) {
			seen[property] = 1
			add_row(property, prop_value[dataset, property], prop_source[dataset, property])
		}
	}
	for (ancestor = parent_of(dataset); ancestor != ""; ancestor = parent_of(ancestor)) {
		for (k = 1; k <= prop_count[ancestor]; k++) {
			property = prop_at[ancestor, k]
			if (!(property in seen) && ((ancestor, property) in prop_source) &&
				prop_source[ancestor, property] == "local") {
				seen[property] = 1
				add_row(property, prop_value[ancestor, property], "inherited from " ancestor)
			}
		}
	}
	if (dataset in ds_volsize) {
		add_row("volsize", ds_volsize[dataset], "local")
		add_row("volblocksize", "16384", "default")
		return
	}
	add_row("casesensitivity", "sensitive", "-")
	add_row("normalization", "none", "-")
	add_row("utf8only", "off", "-")
}

function add_row(name, value, source) {
	row_name[++row_count] = name
	row_value[row_count] = value
	row_source[row_count] = source
}

# Fills sorted_ds[1..sorted_count] with the live datasets in byte order, so a
# parent always precedes its children.
function sort_datasets(    name, i, held) {
	sorted_count = 0
	for (name in ds_guid) {
		held = name ""
		for (i = ++sorted_count; i > 1 && sorted_ds[i - 1] > held; i--)
			sorted_ds[i] = sorted_ds[i - 1]
		sorted_ds[i] = held
	}
}

# Returns the depth of NAME below ROOT (0 for ROOT itself), or -1 when NAME
# is outside ROOT's tree.
function depth_below(root, name,    rest) {
	if (same(name, root))
		return 0
	if (!same(substr(name, 1, length(root) + 1), root "/"))
		return -1
	rest = substr(name, length(root) + 2)
	return gsub(/\//, "/", rest) + 1
}

# "[arg] [arg] ..." for one escaped, TAB-separated argv log line.
function render(line,    parts, n, i, out) {
	n = split(line, parts, "\t")
	out = ""
	for (i = 1; i <= n; i++)
		out = out (i > 1 ? " " : "") "[" parts[i] "]"
	return out
}

################################################################################
# gen: one seeded case.
#
# Files written into ARGV_FUZZ_CASE_DIR:
#   model     the starting model for the fake zfs
#   operands  line 1 the source root, line 2 the destination root (both
#             printable), line 3 "source" or "destination" (the operand the
#             invalid mode breaks), line 4 that broken operand for printf %b
#   expect    "map<TAB>dest<TAB>source" per replicated dataset,
#             "prop<TAB>name" per generated user property,
#             "must"/"never"<TAB>dest<TAB>property for property passes, and
#             "keep<TAB>dest<TAB>property<TAB>=value" (or "-" for unset) for
#             each destination whose source lacks the property: the value
#             it shows before the run, which a property pass must keep
#   summary   the case in readable form for failure reports
# A Park-Miller generator stands in for srand/rand, so a seed produces the
# same case with every awk.

function gen_main(    i, j, p, parent, name, dst_pool, prop) {
	case_dir = ENVIRON["ARGV_FUZZ_CASE_DIR"]
	case_number = ENVIRON["ARGV_FUZZ_CASE"] % 10000
	rng_start(ENVIRON["ARGV_FUZZ_SEED"], ENVIRON["ARGV_FUZZ_CASE"])
	alnum = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	# Escaped and blank-separated: BWK awk's split() also splits on newlines.
	required_count = split_escaped("\\011 \\012 \\015 \\001 ' \" \\\\ $( ` % , =", required)
	special_count = split_escaped("\\011 \\012 \\015 \\001 \\177 ' \" \\\\ $( ) ` % , = ; \\040 \\040\\040 $ * ? [ ] { } & && | < > ! # ~ - -- %25 %2C %3D %0A \\\\n \\\\t '\\\\'' $(id) ${x} \\\\001", special)
	# One case in three keeps every value on one line (a LF becomes a TAB),
	# so the recursive prefetch, which leaves a tree with a multi-line value
	# to per-dataset reads, publishes its rows.
	one_line = rnd(3) == 0

	src_pool = gen_pool()
	do
		dst_pool = gen_pool()
	while (same(dst_pool, src_pool))
	name = src_pool
	for (i = 1 + rnd(3); i > 0; i--) {
		gen_dataset(name)
		name = name "/" gen_component(14)
	}
	src_root = name
	dst_root = dst_pool
	if (rnd(2))
		dst_root = dst_root "/" gen_component(14)
	gen_dataset(dst_pool)
	if (!same(dst_root, dst_pool))
		gen_dataset(dst_root)
	mapped_root = dst_root "/" substr(src_root, length(parent_of(src_root)) + 2)

	# The replicated tree: index 0 is the root and children follow their
	# parents. One time in three a child extends the name of its parent's
	# previous child ("a" then "a b", "axy", "a-b" or "a.b"), so one sibling's
	# name is a prefix of another's. A child is missing on the destination
	# when its parent is, or one time in four. A leaf is a volume one time
	# in three.
	src_name[0] = src_root
	dst_name[0] = mapped_root
	dst_missing[0] = 0
	used_name[src_root] = 1
	tree_count = 1 + rnd(4)
	for (i = 1; i <= tree_count; i++) {
		parent = rnd(i)
		do {
			if ((parent in last_child) && rnd(3) == 0)
				name = src_name[last_child[parent]] substr(" x-.", 1 + rnd(4), 1) gen_component(4)
			else
				name = src_name[parent] "/" gen_component(12)
		} while (name in used_name)
		used_name[name] = 1
		last_child[parent] = i
		src_name[i] = name
		src_parent[i] = parent
		dst_name[i] = mapped_root substr(name, length(src_root) + 1)
		dst_missing[i] = dst_missing[parent] || rnd(4) == 0
	}
	for (i = 1; i <= tree_count; i++)
		if (!(i in last_child) && rnd(3) == 0)
			volsize[i] = 1048576 * (1 + rnd(64))
	snap_total = 2 + rnd(2)
	for (j = 1; j <= snap_total; j++) {
		do
			name = gen_component(12)
		while (name in used_snap)
		used_snap[name] = 1
		snap_name[j] = name
	}
	# Every destination dataset lacks the newest snapshot.
	for (i = 0; i <= tree_count; i++) {
		gen_dataset(src_name[i])
		if (i in volsize)
			model_line("V", src_name[i], volsize[i])
		for (j = 1; j <= snap_total; j++)
			model_line("S", src_name[i], snap_name[j], snapshot_guid(i, j), 1700000000 + j * 100 + i)
		expect_line("map", dst_name[i], src_name[i])
		summary_dataset(i)
		if (dst_missing[i]) {
			model_line("M", dst_name[i])
			continue
		}
		gen_dataset(dst_name[i])
		if (i in volsize)
			model_line("V", dst_name[i], volsize[i])
		for (j = 1; j < snap_total; j++)
			model_line("S", dst_name[i], snap_name[j], snapshot_guid(i, j), 1700000000 + j * 100 + i)
	}
	model_line("P", src_root, "compression", "lz4", "local")
	model_line("P", mapped_root, "compression", "lz4", "local")

	prop_total = 1 + rnd(3)
	for (p = 1; p <= prop_total; p++) {
		do
			prop = "fz" p ":" gen_property_suffix()
		while (prop in used_prop)
		used_prop[prop] = 1
		prop_name[++prop_named] = prop
		expect_line("prop", prop)
		gen_property(prop, p == 1)
	}
	gen_invalid_operand()
	write_summary()
}

function split_escaped(list, items,    n, i) {
	n = split(list, items, " ")
	for (i = 1; i <= n; i++)
		items[i] = unesc(items[i])
	return n
}

# Park-Miller: state = state * 48271 mod 2^31-1. A case starts at its own
# point of that one stream, jumped to by exponentiation: seeds are scattered
# 1234567891 steps apart and cases 2^20 steps apart, far more than a case
# draws, so neighbouring cases do not echo each other.
function rng_start(seed, case_index,    period, position) {
	period = 2147483646
	position = (mulmod(seed % period, 1234567891, period) + mulmod(case_index % period, 1048576, period)) % period
	rng_state = powmod(48271, position)
}

# Returns a pseudo-random integer in [0, n).
function rnd(n) {
	rng_state = (rng_state * 48271) % 2147483647
	return int(rng_state * n / 2147483647)
}

# x * y mod m for x, y, m below 2^31, split so no product passes 2^53 and
# every awk computes it exactly.
function mulmod(x, y, m) {
	return ((x * int(y / 65536)) % m * 65536 + x * (y % 65536)) % m
}

function powmod(base, exponent,    result) {
	result = 1
	while (exponent > 0) {
		if (exponent % 2)
			result = mulmod(result, base, 2147483647)
		base = mulmod(base, base, 2147483647)
		exponent = int(exponent / 2)
	}
	return result
}

# One dataset-name component: alnum plus space - _ . :, with no leading or
# trailing space, and never "." or "..".
function gen_component(max_length,    wanted, text, i) {
	do {
		wanted = 1 + rnd(max_length)
		text = ""
		for (i = 1; i <= wanted; i++) {
			if (rnd(3) == 0)
				text = text substr(" -_.:", 1 + rnd(5), 1)
			else
				text = text substr(alnum, 1 + rnd(62), 1)
		}
	} while (text ~ /^ / || text ~ / $/ || text == "." || text == "..")
	return text
}

# A pool name starts with a letter other than c and avoids the names zpool
# reserves for vdev types.
function gen_pool(    name) {
	do
		name = substr("abdefghijklmnopqrstuvwxyzABDEFGHIJKLMNOPQRSTUVWXYZ", 1 + rnd(50), 1) gen_component(10)
	while (name ~ /^(mirror|raidz|draid)/ || name == "spare" || name == "log")
	return name
}

function gen_property_suffix(    text, i, n) {
	text = ""
	n = 1 + rnd(8)
	for (i = 1; i <= n; i++)
		text = text substr("abcdefghijklmnopqrstuvwxyz0123456789:._-", 1 + rnd(40), 1)
	return text
}

function gen_dataset(name) {
	model_line("D", name, "5" sprintf("%04d%04d", case_number, ++dataset_total) "00000003")
}

function snapshot_guid(tree_index, snap_index) {
	return "1" sprintf("%04d%02d%02d", case_number, tree_index, snap_index) "000000007"
}

# A property value of bytes 0x01-0x7f, about half of its pieces taken from
# the shell and serialization specials, and one piece in three shaped like a
# `zfs get` record. WITH_REQUIRED puts every required special and one
# record-shaped piece in once, shuffled, so each case covers all of them (a
# one-line case turns each LF into a TAB).
function gen_value(with_required,    text, i, n, order, k, held) {
	text = ""
	if (with_required) {
		for (i = 1; i <= required_count; i++)
			order[i] = required[i]
		order[required_count + 1] = record_shaped()
		for (i = required_count + 1; i > 1; i--) {
			k = 1 + rnd(i)
			held = order[i]
			order[i] = order[k]
			order[k] = held
		}
		for (i = 1; i <= required_count + 1; i++)
			text = text order[i] (rnd(2) ? sprintf("%c", 32 + rnd(95)) : "")
	}
	n = 1 + rnd(12)
	for (i = 1; i <= n; i++) {
		k = rnd(6)
		if (k == 0)
			text = text record_shaped()
		else if (k < 3)
			text = text special[1 + rnd(special_count)]
		else
			text = text sprintf("%c", 1 + rnd(127))
	}
	if (one_line)
		gsub(/\n/, "\t", text)
	return text
}

# `zfs get -H` prints values raw, so this piece makes the value's next line
# read exactly like another record: TAB, a source word, LF, then a property
# name (after a source or destination dataset name, as a recursive listing
# prints it) and a TAB. The name is a fake user property, a native one, the
# next real record's name, or a generated property, so a parser that guesses
# record boundaries injects or truncates a value.
function record_shaped(    words, source, name, k) {
	split("local - default received", words, " ")
	source = rnd(5) ? words[1 + rnd(4)] : "inherited from " src_root
	split("fzx:evil compression readonly casesensitivity utf8only volsize type", words, " ")
	name = rnd(3) ? words[1 + rnd(7)] : prop_name[1 + rnd(prop_named)]
	if (rnd(2)) {
		k = rnd(tree_count + 1)
		name = (rnd(2) ? src_name[k] : dst_name[k]) "\t" name
	}
	return "\t" source "\n" name "\t"
}

# One user property across the tree. Source: it starts on the root, or one
# time in four on a child instead, so the datasets outside that child's
# subtree lack it and may hold no user property at all. It is local where it
# starts, and below that each dataset inherits it, sets its own value, or
# sets its parent's value. Destination: the -R target may hold a local value
# the tree inherits; each existing dataset has no row, the source's value
# (only where the source has the property), or another value. A -P pass must
# set every dataset whose source row is local, unless the destination already
# holds the same value locally, and must leave the property alone where the
# source lacks it.
function gen_property(prop, with_required,    start, i, parent, choice, value, dest_same, dest_own, root_has, root_value) {
	start = rnd(4) ? 0 : 1 + rnd(tree_count)
	for (i = 0; i <= tree_count; i++) {
		parent = src_parent[i]
		src_has[i] = (i == start) || (i > 0 && src_has[parent])
		src_local[i] = 0
		if (!src_has[i])
			continue
		choice = (i == start) ? 1 : rnd(3)
		src_local[i] = choice != 0
		src_value[i] = (choice == 1) ? gen_value(with_required && i == start) : src_value[parent]
		if (src_local[i]) {
			model_line("P", src_name[i], prop, src_value[i], "local")
			summary_property(prop, src_name[i], src_value[i])
		}
	}
	root_has = rnd(2)
	if (root_has) {
		root_value = gen_value(0)
		model_line("P", dst_root, prop, root_value, "local")
		summary_property(prop, dst_root, root_value)
	}
	for (i = 0; i <= tree_count; i++) {
		dest_same = 0
		dest_own = 0
		if (!dst_missing[i]) {
			choice = rnd(3)
			# The copy of the value holding every required byte always
			# differs, so that value always reaches zfs set.
			if (choice == 1 && ((with_required && i == start) || !src_has[i]))
				choice = 2
			if (choice != 0) {
				value = (choice == 1) ? src_value[i] : gen_value(0)
				dest_same = choice == 1
				dest_own = 1
				model_line("P", dst_name[i], prop, value, "local")
				summary_property(prop, dst_name[i], value)
			}
		}
		# The value the destination shows before the run: its own, else
		# its parent's, else the -R target's.
		if (dest_own) {
			dest_has[i] = 1
			dest_value[i] = value
		} else if (i > 0) {
			dest_has[i] = dest_has[src_parent[i]]
			dest_value[i] = dest_value[src_parent[i]]
		} else {
			dest_has[i] = root_has
			dest_value[i] = root_value
		}
		if (!src_has[i])
			print "keep\t" esc(dst_name[i]) "\t" prop "\t" (dest_has[i] ? "=" esc(dest_value[i]) : "-") > (case_dir "/expect")
		else if (src_local[i])
			expect_line(dest_same ? "never" : "must", dst_name[i], prop)
	}
}

# The invalid mode replaces one operand with a copy holding a control byte.
function gen_invalid_operand(    which, name, at, bytes) {
	which = rnd(2) ? "source" : "destination"
	name = (which == "source") ? src_root : dst_root
	split("011 012 015 001 177", bytes, " ")
	at = 2 + rnd(length(name) - 1)
	operands_line(src_root)
	operands_line(dst_root)
	operands_line(which)
	operands_line(substr(name, 1, at - 1) "\\0" bytes[1 + rnd(5)] substr(name, at))
}

function model_line(kind, name, x, y, z,    line) {
	line = kind "\t" esc(name)
	if (kind == "D" || kind == "V")
		line = line "\t" x
	else if (kind == "S")
		line = line "\t" esc(x) "\t" y "\t" z
	else if (kind == "P")
		line = line "\t" x "\t" esc(y) "\t" z
	print line > (case_dir "/model")
}

function expect_line(kind, x, y) {
	print kind "\t" esc(x) ((y == "") ? "" : "\t" esc(y)) > (case_dir "/expect")
}

function operands_line(text) {
	print text > (case_dir "/operands")
}

function summary_dataset(i) {
	summary_tree = summary_tree "  [" esc(src_name[i]) "] -> [" esc(dst_name[i]) "]" \
		((i in volsize) ? " (volume)" : "") \
		(dst_missing[i] ? " (missing on the destination)" : "") "\n"
}

function summary_property(prop, dataset, value) {
	summary_props = summary_props "  " prop " on [" esc(dataset) "]: [" esc(value) "]\n"
}

function write_summary(    j, snaps) {
	snaps = ""
	for (j = 1; j <= snap_total; j++)
		snaps = snaps " [" esc(snap_name[j]) "]"
	printf "source [%s], destination [%s]\ndatasets:\n%ssnapshots:%s\nlocal properties%s:\n%s", \
		esc(src_root), esc(dst_root), summary_tree, snaps, one_line ? " (one line each)" : "", \
		summary_props > (case_dir "/summary")
}

################################################################################
# zfs: the fake zfs. It answers from the model, appends its mutations to it,
# and records a violation for any operand that is not a whole generated name.

function zfs_main(    i, command) {
	state_dir = ENVIRON["ARGV_FUZZ_STATE"]
	model_file = state_dir "/model"
	word_count = ARGC - 1
	argv_line = ""
	for (i = 1; i <= word_count; i++) {
		word[i] = ARGV[i]
		ARGV[i] = ""
		argv_line = argv_line ((i > 1) ? "\t" : "") esc(word[i])
	}
	print argv_line >> (state_dir "/argv.log")
	close(state_dir "/argv.log")
	start_race(state_dir "/race")
	load_model(model_file)
	mutations = ""

	command = word[1]
	if (command == "list")
		status = zfs_list()
	else if (command == "get")
		status = zfs_get()
	else if (command == "set")
		status = zfs_set()
	else if (command == "inherit")
		status = zfs_inherit()
	else if (command == "create")
		status = zfs_create()
	else if (command == "send")
		status = zfs_send()
	else if (command == "receive" || command == "recv")
		status = zfs_receive()
	else {
		# destroy, rollback and the rest have no place in these cases.
		violation("unsupported zfs subcommand")
		status = 2
	}

	if (mutations != "") {
		printf "%s", mutations >> model_file
		close(model_file)
	}
	return status
}

# A race (black-box pins only): when the file RACE starts with this call's
# escaped argv line, the rest of it is model lines that change the model just
# before the call answers, as another process would. It fires once.
function start_race(file,    trigger, line, lines) {
	if ((getline trigger < file) <= 0 || !same(trigger, argv_line)) {
		close(file)
		return
	}
	lines = ""
	while ((getline line < file) > 0)
		lines = lines line "\n"
	close(file)
	printf "%s", lines >> model_file
	close(model_file)
	printf "" > file
	close(file)
}

function violation(message) {
	print "fake zfs: " message > "/dev/stderr"
	print message ": " render(argv_line) >> (state_dir "/violations")
	close(state_dir "/violations")
}

function mutate(kind, name, x, y, z) {
	if (kind == "D" || kind == "V")
		mutations = mutations kind "\t" esc(name) "\t" x "\n"
	else if (kind == "S")
		mutations = mutations "S\t" esc(name) "\t" esc(x) "\t" y "\t" z "\n"
	else if (kind == "P")
		mutations = mutations "P\t" esc(name) "\t" x "\t" esc(y) "\tlocal\n"
	else
		mutations = mutations "X\t" esc(name) "\t" x "\n"
}

# Every operand must be a whole generated name: a dataset the case knows, or
# one of its snapshots under a generated snapshot name. Anything else means
# an argument boundary broke on the way here.
function check_operand(name,    at, dataset) {
	if ((name in ds_guid) || (name in creatable))
		return
	at = index(name, "@")
	dataset = substr(name, 1, at - 1)
	if (at > 1 && ((dataset in ds_guid) || (dataset in creatable)) && (substr(name, at + 1) in snap_names))
		return
	violation("operand is not a generated name: [" esc(name) "]")
}

function check_operands(from,    i) {
	for (i = from; i <= word_count; i++)
		check_operand(word[i])
}

function missing(name) {
	print "cannot open '" name "': dataset does not exist" > "/dev/stderr"
	return 1
}

function usage(message) {
	print "fake zfs: usage: " message > "/dev/stderr"
	return 2
}

# Parses zfs options from word[start]: letters in WITH_VALUE take the rest of
# their cluster or the next word. Sets opt_flag[letter], opt_value[letter]
# (the last value) and pair[1..pair_count] (every -o value). Returns the
# index of the first operand, or 0 when an option value is missing.
function parse_options(start, with_value,    i, j, arg, letter, value) {
	split("", opt_flag)
	split("", opt_value)
	pair_count = 0
	for (i = start; i <= word_count; i++) {
		arg = word[i]
		if (arg == "--")
			return i + 1
		if (substr(arg, 1, 1) != "-" || arg == "-")
			return i
		for (j = 2; j <= length(arg); j++) {
			letter = substr(arg, j, 1)
			opt_flag[letter] = 1
			if (!index(with_value, letter))
				continue
			value = substr(arg, j + 1)
			if (value == "") {
				if (++i > word_count)
					return 0
				value = word[i]
			}
			opt_value[letter] = value
			if (letter == "o")
				pair[++pair_count] = value
			break
		}
	}
	return i
}

function has_word(list, item) {
	return index("," list ",", "," item ",") > 0
}

function zfs_list(    first, types, want_type, want_snap, depth, nfields, i, k, name, status, d) {
	first = parse_options(2, "odstS")
	if (!first)
		return usage("option needs a value")
	types = ("t" in opt_value) ? opt_value["t"] : "filesystem,volume"
	want_type["filesystem"] = has_word(types, "filesystem") || has_word(types, "all")
	want_type["volume"] = has_word(types, "volume") || has_word(types, "all")
	want_snap = has_word(types, "snapshot") || has_word(types, "snap") || has_word(types, "all")
	depth = ("d" in opt_value) ? opt_value["d"] + 0 : (("r" in opt_flag) ? 1000000 : -1)
	nfields = split(("o" in opt_value) ? opt_value["o"] : "name,used,avail,refer,mountpoint", field, ",")
	check_operands(first)
	sort_datasets()
	status = 0
	out_count = 0
	for (i = first; i <= word_count; i++) {
		name = word[i]
		if (index(name, "@")) {
			if (!(name in snap_guid))
				status = missing(name)
			else if (want_snap)
				out_row[++out_count] = name
			continue
		}
		if (!(name in ds_guid)) {
			status = missing(name)
			continue
		}
		for (k = 1; k <= sorted_count; k++) {
			d = depth_below(name, sorted_ds[k])
			if (d < 0)
				continue
			if (want_type[dataset_type(sorted_ds[k])] && d <= ((depth < 0) ? 0 : depth))
				out_row[++out_count] = sorted_ds[k]
			if (want_snap && d + 1 <= ((depth < 0) ? 1 : depth))
				add_snapshot_rows(sorted_ds[k])
		}
	}
	if (opt_value["s"] == "creation")
		sort_rows_by_creation()
	for (i = 1; i <= out_count; i++)
		print_list_row(out_row[i], nfields)
	return status
}

function add_snapshot_rows(dataset,    k, first, j, held) {
	first = out_count + 1
	for (k = 1; k <= snap_count[dataset]; k++) {
		held = dataset "@" snap_at[dataset, k]
		for (j = ++out_count; j > first && snap_time[out_row[j - 1]] > snap_time[held]; j--)
			out_row[j] = out_row[j - 1]
		out_row[j] = held
	}
}

function row_time(name) {
	return index(name, "@") ? snap_time[name] : 1600000000
}

function sort_rows_by_creation(    i, j, held) {
	for (i = 2; i <= out_count; i++) {
		held = out_row[i]
		for (j = i; j > 1 && row_time(out_row[j - 1]) > row_time(held); j--)
			out_row[j] = out_row[j - 1]
		out_row[j] = held
	}
}

function print_list_row(name, nfields,    line, i, f, snapshot, value) {
	snapshot = index(name, "@") > 0
	line = ""
	for (i = 1; i <= nfields; i++) {
		f = field[i]
		if (f == "name")
			value = name
		else if (f == "guid")
			value = snapshot ? snap_guid[name] : ds_guid[name]
		else if (f == "creation" || f == "createtxg")
			value = row_time(name)
		else if (f == "type")
			value = snapshot ? "snapshot" : dataset_type(name)
		else if (f == "used")
			value = "96K"
		else if (f == "avail" || f == "available")
			value = snapshot ? "-" : "1.0G"
		else if (f == "refer" || f == "referenced")
			value = "24K"
		else if (f == "mountpoint")
			value = (snapshot || (name in ds_volsize)) ? "-" : "/" name
		else if (!snapshot && effective(name, f))
			value = eff_value
		else
			value = "-"
		line = line ((i > 1) ? "\t" : "") value
	}
	print line
}

function native_property(name) {
	return has_word("type,guid,creation,createtxg,compression,casesensitivity,normalization,utf8only,volsize,volblocksize,mountpoint,atime,readonly,canmount,recordsize,used,available,referenced", name)
}

# Dataset properties only: zxfer reads no snapshot properties here, and its
# -t and -s filters would not change the rows. A state directory holding
# refuse_recursive_get (mode L) fails every -r read, as a zfs that cannot
# list the tree would, so zxfer reads each dataset alone.
function zfs_get(    first, wanted, nwanted, nfields, depth, i, k, name, status, d, refused, flag) {
	first = parse_options(2, "odts")
	if (!first || first > word_count)
		return usage("get needs a property list")
	if ("r" in opt_flag) {
		refused = (getline flag < (state_dir "/refuse_recursive_get")) >= 0
		close(state_dir "/refuse_recursive_get")
		if (refused) {
			print "fake zfs: this run refuses recursive reads" > "/dev/stderr"
			return 1
		}
	}
	check_operands(first + 1)
	nwanted = split(word[first], wanted, ",")
	for (i = 1; i <= nwanted; i++) {
		if (wanted[i] != "all" && !(wanted[i] in known_property) && !native_property(wanted[i]) &&
			!(index(wanted[i], ":") && wanted[i] ~ /^[a-z0-9:._-]+$/)) {
			print "bad property list: invalid property '" wanted[i] "'" > "/dev/stderr"
			return 2
		}
	}
	nfields = split(("o" in opt_value) ? opt_value["o"] : "name,property,value,source", field, ",")
	depth = ("d" in opt_value) ? opt_value["d"] + 0 : (("r" in opt_flag) ? 1000000 : 0)
	sort_datasets()
	status = 0
	for (i = first + 1; i <= word_count; i++) {
		name = word[i]
		if (!(name in ds_guid)) {
			status = missing(name)
			continue
		}
		for (k = 1; k <= sorted_count; k++) {
			d = depth_below(name, sorted_ds[k])
			if (d < 0 || d > depth)
				continue
			collect_properties(sorted_ds[k])
			print_get_rows(sorted_ds[k], wanted, nwanted, nfields)
		}
	}
	return status
}

function print_get_rows(name, wanted, nwanted, nfields,    i, k, found) {
	for (i = 1; i <= nwanted; i++) {
		if (wanted[i] == "all") {
			for (k = 1; k <= row_count; k++)
				print_get_row(name, row_name[k], row_value[k], row_source[k], nfields)
			continue
		}
		found = 0
		for (k = 1; k <= row_count && !found; k++) {
			if (row_name[k] == wanted[i]) {
				print_get_row(name, row_name[k], row_value[k], row_source[k], nfields)
				found = 1
			}
		}
		if (!found)
			print_get_row(name, wanted[i], "-", "-", nfields)
	}
}

# One `zfs get` row: the value is printed raw, tabs and newlines included,
# exactly as zfs does.
function print_get_row(name, property, value, source, nfields,    line, i, f) {
	line = ""
	for (i = 1; i <= nfields; i++) {
		f = field[i]
		if (i > 1)
			line = line "\t"
		if (f == "name")
			line = line name
		else if (f == "property")
			line = line property
		else if (f == "value")
			line = line value
		else if (f == "source")
			line = line source
		else
			line = line "-"
	}
	print line
}

# A property argument must name a property the case knows; a boundary that
# broke inside a value could otherwise pass as another property.
function check_property_name(property) {
	if ((property in known_property) || native_property(property))
		return 1
	violation("unknown property: [" esc(property) "]")
	return 0
}

# zfs set takes property=value words up to the first word without "=", and
# dataset operands after it.
function zfs_set(    first, i, k, n, eq, status) {
	first = 2
	while (first <= word_count && index(word[first], "="))
		first++
	check_operands(first)
	n = 0
	for (i = 2; i < first; i++) {
		eq = index(word[i], "=")
		set_name[++n] = substr(word[i], 1, eq - 1)
		set_value[n] = substr(word[i], eq + 1)
		if (!check_property_name(set_name[n]))
			return 1
	}
	if (n == 0 || first > word_count)
		return usage("set needs property=value and a dataset")
	status = 0
	for (i = first; i <= word_count; i++) {
		if (!(word[i] in ds_guid)) {
			status = missing(word[i])
			continue
		}
		for (k = 1; k <= n; k++)
			mutate("P", word[i], set_name[k], set_value[k])
	}
	return status
}

function zfs_inherit(    first, i, status) {
	first = parse_options(2, "")
	if (!first || first >= word_count)
		return usage("inherit needs a property and a dataset")
	check_operands(first + 1)
	if (!check_property_name(word[first]))
		return 1
	status = 0
	for (i = first + 1; i <= word_count; i++) {
		if (!(word[i] in ds_guid))
			status = missing(word[i])
		else
			mutate("X", word[i], word[first])
	}
	return status
}

function zfs_create(    first, name, ancestor, k, eq, property) {
	first = parse_options(2, "oV")
	if (first)
		check_operands(first)
	if (!first || first != word_count)
		return usage("create needs exactly one dataset")
	name = word[word_count]
	if (name in ds_guid) {
		print "cannot create '" name "': dataset already exists" > "/dev/stderr"
		return 1
	}
	for (k = 1; k <= pair_count; k++) {
		eq = index(pair[k], "=")
		if (eq < 2)
			return usage("-o needs property=value")
		if (!check_property_name(substr(pair[k], 1, eq - 1)))
			return 1
	}
	for (ancestor = parent_of(name); ancestor != "" && !(ancestor in ds_guid); ancestor = parent_of(ancestor)) {
		if (!("p" in opt_flag))
			return missing(ancestor)
		mutate("D", ancestor, "3000000000000000001")
	}
	if (ancestor == "")
		return missing(name)
	mutate("D", name, "3000000000000000001")
	if ("V" in opt_value)
		mutate("V", name, opt_value["V"])
	for (k = 1; k <= pair_count; k++) {
		eq = index(pair[k], "=")
		property = substr(pair[k], 1, eq - 1)
		if (!has_word("casesensitivity,normalization,utf8only", property))
			mutate("P", name, property, substr(pair[k], eq + 1))
	}
	return 0
}

# The stream is escaped text: a header naming the source dataset, "V volsize"
# for a volume, "B guid" for an incremental base, and one "S snap guid time"
# line per snapshot.
function zfs_send(    first, target, dataset, base, at, k, name, from_time) {
	first = parse_options(2, "iItX")
	if (first)
		check_operands(first)
	base = ("I" in opt_value) ? opt_value["I"] : opt_value["i"]
	if (base != "" && substr(base, 1, 1) != "@")
		check_operand(base)
	if (!first || first != word_count)
		return usage("send needs exactly one snapshot")
	target = word[word_count]
	if (!(target in snap_guid))
		return missing(target)
	at = index(target, "@")
	dataset = substr(target, 1, at - 1)
	if (substr(base, 1, 1) == "@")
		base = dataset base
	if (base != "") {
		if (!(base in snap_guid) || !same(substr(base, 1, at), dataset "@") || snap_time[base] >= snap_time[target])
			return usage("bad incremental source: " base)
	}
	if ("n" in opt_flag)
		return 0
	print "ZXFERFUZZSTREAM\t" esc(dataset)
	if (dataset in ds_volsize)
		print "V\t" ds_volsize[dataset]
	if (base != "")
		print "B\t" snap_guid[base]
	from_time = ("I" in opt_value) ? snap_time[base] : snap_time[target] - 1
	for (k = 1; k <= snap_count[dataset]; k++) {
		name = dataset "@" snap_at[dataset, k]
		if (snap_time[name] > from_time && snap_time[name] <= snap_time[target])
			print "S\t" esc(snap_at[dataset, k]) "\t" snap_guid[name] "\t" snap_time[name]
	}
	return 0
}

# Reads the whole stream from stdin, then applies it like zfs receive: an
# incremental needs its base as the target's newest snapshot, and a full
# stream a missing target (or an empty one under -F); an existing target
# must have the stream's type.
function zfs_receive(    first, target, line, f, header, stream_volsize, base, n, k, newest) {
	first = parse_options(2, "ox")
	if (first)
		check_operands(first)
	if (!first || first != word_count)
		return usage("receive needs exactly one target")
	target = word[word_count]
	header = ""
	stream_volsize = ""
	base = ""
	n = 0
	while ((getline line) > 0) {
		split(line, f, "\t")
		if (header == "")
			header = f[1]
		else if (f[1] == "V")
			stream_volsize = f[2]
		else if (f[1] == "B")
			base = f[2]
		else if (f[1] == "S") {
			recv_snap[++n] = unesc(f[2])
			recv_guid[n] = f[3]
			recv_time[n] = f[4]
		}
	}
	if (header != "ZXFERFUZZSTREAM" || n == 0) {
		print "cannot receive: invalid stream (bad magic number)" > "/dev/stderr"
		return 1
	}
	if ((target in ds_guid) && (target in ds_volsize) != (stream_volsize != "")) {
		print "cannot receive: destination " target " is a " dataset_type(target) \
			", the stream is not" > "/dev/stderr"
		return 1
	}
	if (base != "") {
		if (!(target in ds_guid))
			return missing(target)
		newest = target "@" snap_at[target, snap_count[target]]
		if (!snap_count[target] || !same(snap_guid[newest], base)) {
			print "cannot receive incremental stream: destination " target " has been modified" > "/dev/stderr"
			return 1
		}
	} else if (target in ds_guid) {
		if (!("F" in opt_flag) || snap_count[target] > 0) {
			print "cannot receive new filesystem stream: destination '" target "' exists" > "/dev/stderr"
			return 1
		}
	} else if (!(parent_of(target) in ds_guid)) {
		return missing(parent_of(target))
	} else {
		mutate("D", target, "4000000000000000001")
		if (stream_volsize != "")
			mutate("V", target, stream_volsize)
	}
	for (k = 1; k <= n; k++)
		mutate("S", target, recv_snap[k], recv_guid[k], recv_time[k])
	return 0
}

################################################################################
# check: compare a finished run with the case's expectations.
#
# Environment: ARGV_FUZZ_CASE_DIR (the case), ARGV_FUZZ_STATE (the run's
# final model, argv.log and violations), ARGV_FUZZ_MODE, ARGV_FUZZ_STATUS
# (zxfer's exit status) and ARGV_FUZZ_STDERR (zxfer's stderr file). Every
# mode but R runs a -P property pass. Mode L, whose property reads are all
# per-dataset, must also mutate exactly as mode P, whose reads start with
# the recursive prefetch.

function check_main(    mode) {
	case_dir = ENVIRON["ARGV_FUZZ_CASE_DIR"]
	state_dir = ENVIRON["ARGV_FUZZ_STATE"]
	mode = ENVIRON["ARGV_FUZZ_MODE"]
	problems = 0
	read_argv_log()
	if (mode == "invalid")
		check_invalid_run(ENVIRON["ARGV_FUZZ_STATUS"] + 0)
	else
		check_valid_run(ENVIRON["ARGV_FUZZ_STATUS"] + 0, mode != "R")
	if (mode == "L")
		check_same_mutations(case_dir "/P/state/argv.log")
	return problems > 0
}

function is_mutation(line) {
	return line ~ /^(create|set|inherit)(\t|$)/
}

# The create, set and inherit calls of this run and of the run that logged
# OTHER_LOG must match one for one: the same property lists, whichever way
# they were read, make the same changes.
function check_same_mutations(other_log,    line, other_count, other, c, mine) {
	other_count = 0
	while ((getline line < other_log) > 0)
		if (is_mutation(line))
			other[++other_count] = line
	close(other_log)
	mine = 0
	for (c = 1; c <= call_count; c++) {
		if (!is_mutation(call_line[c]))
			continue
		if (++mine > other_count || !same(call_line[c], other[mine])) {
			problem("per-dataset reads (L) and the recursive prefetch (P) differ at mutation " mine ": L " render(call_line[c]) ", P " ((mine > other_count) ? "none" : render(other[mine])))
			return
		}
	}
	if (mine < other_count)
		problem("per-dataset reads (L) and the recursive prefetch (P) differ at mutation " (mine + 1) ": L none, P " render(other[mine + 1]))
}

function problem(message) {
	print message
	problems++
}

function read_argv_log(    file, line, n, i) {
	file = state_dir "/argv.log"
	call_count = 0
	while ((getline line < file) > 0) {
		call_line[++call_count] = line
		n = split(line, call_part, "\t")
		call_argc[call_count] = n
		for (i = 1; i <= n; i++)
			call_arg[call_count, i] = unesc(call_part[i])
	}
	close(file)
}

# An operand with a control byte must fail closed: a non-zero exit, a
# structured failure report, and no zfs call beyond discovery.
function check_invalid_run(run_status,    file, line, begin, end, c) {
	if (run_status == 0)
		problem("zxfer exited 0 for an operand holding a control byte")
	file = ENVIRON["ARGV_FUZZ_STDERR"]
	while ((getline line < file) > 0) {
		if (line == "zxfer: failure report begin")
			begin = 1
		if (line == "zxfer: failure report end")
			end = 1
	}
	close(file)
	if (!begin || !end)
		problem("stderr holds no structured failure report")
	for (c = 1; c <= call_count; c++)
		if (call_arg[c, 1] != "list" && call_arg[c, 1] != "get")
			problem("zfs " call_arg[c, 1] " ran for an invalid operand: " render(call_line[c]))
}

function check_valid_run(run_status, property_pass,    file, line, f, c, n, i, first_dataset, target, command, dest, prop, key) {
	if (run_status != 0)
		problem("zxfer exited " run_status " (want 0)")
	file = state_dir "/violations"
	while ((getline line < file) > 0)
		problem("fake zfs: " line)
	close(file)

	file = case_dir "/expect"
	while ((getline line < file) > 0) {
		split(line, f, "\t")
		if (f[1] == "map")
			source_of[unesc(f[2])] = unesc(f[3])
		else if (f[1] == "prop")
			user_prop[f[2]] = 1
		else if (f[1] == "keep")
			keep_value[unesc(f[2]), f[3]] = (f[4] == "-") ? "" : "=" unesc(substr(f[4], 2))
		else
			expected[unesc(f[2]), f[3]] = f[1]
	}
	close(file)
	load_model(state_dir "/model")
	# The native property=value pairs the fake zfs reports for the sources.
	for (dest in source_of) {
		collect_properties(source_of[dest])
		for (i = 1; i <= row_count; i++)
			if (!(row_name[i] in user_prop))
				source_native_pair[row_name[i] "=" row_value[i]] = 1
	}
	for (dest in source_of)
		for (prop in user_prop)
			if (effective(source_of[dest], prop))
				want[dest, prop] = eff_value

	for (c = 1; c <= call_count; c++) {
		command = call_arg[c, 1]
		n = call_argc[c]
		if (command == "list" || command == "get" || command == "send")
			continue
		if (command == "receive" || command == "recv") {
			received[call_arg[c, n]]++
			for (i = 2; i < n; i++)
				if (call_arg[c, i] == "-o")
					check_property_arg(c, call_arg[c, n], call_arg[c, ++i])
			continue
		}
		if (!property_pass || (command != "set" && command != "create" && command != "inherit")) {
			problem("unexpected zfs " command ": " render(call_line[c]))
			continue
		}
		if (command == "set") {
			first_dataset = 2
			while (first_dataset <= n && index(call_arg[c, first_dataset], "="))
				first_dataset++
			for (target = first_dataset; target <= n; target++)
				for (i = 2; i < first_dataset; i++)
					check_property_arg(c, call_arg[c, target], call_arg[c, i])
		} else if (command == "create") {
			for (i = 2; i < n; i++)
				if (call_arg[c, i] == "-o")
					check_property_arg(c, call_arg[c, n], call_arg[c, ++i])
		}
	}

	for (key in expected) {
		split(key, f, SUBSEP)
		if (property_pass && expected[key] == "must" && !property_set[key])
			problem("-P never set " f[2] " on [" esc(f[1]) "]")
		if (expected[key] == "never" && property_set[key])
			problem("-P set " f[2] " on [" esc(f[1]) "], which already held that value locally")
	}
	for (dest in source_of) {
		if (!received[dest])
			problem("no zfs receive into [" esc(dest) "]")
		check_converged(dest, property_pass)
	}
}

# One property=value argument aimed at DEST: a generated property must carry
# the source's effective value, byte for byte, and any other must be one of
# the native values every source dataset holds, so a record-shaped value can
# never smuggle in a property.
function check_property_arg(c, dest, pair_arg,    eq, prop, value) {
	eq = index(pair_arg, "=")
	prop = substr(pair_arg, 1, eq - 1)
	value = substr(pair_arg, eq + 1)
	if (!(prop in user_prop)) {
		if (!(pair_arg in source_native_pair))
			problem("zfs " call_arg[c, 1] " sets [" esc(pair_arg) "] on [" esc(dest) "], which no source holds: " render(call_line[c]))
		return
	}
	property_set[dest, prop]++
	if (!((dest, prop) in want))
		problem("zfs " call_arg[c, 1] " sets " prop " on [" esc(dest) "], which is not replicated or whose source lacks it: " render(call_line[c]))
	else if (!same(value, want[dest, prop]))
		problem("zfs " call_arg[c, 1] " " prop " on [" esc(dest) "]: got [" esc(value) "] want [" esc(want[dest, prop]) "]")
}

# After the run DEST has its source's type (and volume size) and every source
# snapshot with its guid. After a property pass it shows the source's
# effective value of every generated property the source holds, and the
# value it showed before the run of every other one. Values are compared as
# "=value", or "" when unset.
function check_converged(dest, property_pass,    source, have, wanted, k, name, prop, value) {
	source = source_of[dest]
	have = dataset_type(dest) ((dest in ds_volsize) ? " of " ds_volsize[dest] " bytes" : "")
	wanted = dataset_type(source) ((source in ds_volsize) ? " of " ds_volsize[source] " bytes" : "")
	if (!same(have, wanted))
		problem("[" esc(dest) "] is a " have " after the run, its source a " wanted)
	for (k = 1; k <= snap_count[source]; k++) {
		name = snap_at[source, k]
		if (!((dest "@" name) in snap_guid) || !same(snap_guid[dest "@" name], snap_guid[source "@" name]))
			problem("[" esc(dest) "] lacks snapshot [" esc(name) "] after the run")
	}
	if (!property_pass)
		return
	for (prop in user_prop) {
		value = effective(dest, prop) ? "=" eff_value : ""
		if ((dest, prop) in want) {
			if (!same(value, "=" want[dest, prop]))
				problem("[" esc(dest) "] ends with " prop "[" esc(value) "], the source has [=" esc(want[dest, prop]) "]")
		} else if (!same(value, keep_value[dest, prop])) {
			problem("[" esc(dest) "] ends with " prop "[" esc(value) "], but its source lacks it and it had [" esc(keep_value[dest, prop]) "]")
		}
	}
}
