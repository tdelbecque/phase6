use strict;
use diagnostics;

sub tokenize {
    my ($hl) = @_;
    $hl = " $hl ";
    $hl =~ s/\s+/ /g;
    $hl =~ s=\.\.\.=\n...\n=g;
    $hl =~ s=[/.,;:@#\$%&!?"]=\n$&\n=g;
    $hl =~ s=([^.])([.])([])}>"']*)\s*$=$1\n$2$3\n=g;
    $hl =~ s=[][(){}<>]=\n$&\n=g;
    $hl =~ s=--=\n--\n=g;
    $hl =~ s=([^'])'\s+=$1\n'\n=g;
    $hl =~ s='([sSmMdD])\s+=\n'$1\n=g;
    $hl =~ s='(ll|re|ve)\s+=\n'$1\n=gi;
    $hl =~ s=(n't)\s+=\n$1\n=gi;
    $hl =~ s=\s+(can)(not)\s+=\n$1\n$2\n=gi;
    $hl =~ s=^\s+==;
    $hl =~ s=\s+$==;
    $hl =~ s=\s+=\n=g;
    $hl =~ s=\n+=\n=g;
    $hl;
}
    
while (<>) {
    chomp;
    
	my $tokens = tokenize $_;
    for (split "\n", $tokens) {
        s/^Abstract(.)/$1/;
        $_ = lc;
        if (/^[a-z]+$/ or /^.$/) {
            print "$_\n";
        } elsif (/^\d+$/) {
            print "0\n";
        } elsif (/^\d/) {
            print "NUM\n"
        } elsif (/\d/) {
            print "NUMX\n";
        } else {
            print "CPLX\n"
        } 
	}

}

