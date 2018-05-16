#include <getopt.h>
#include <string>
#include <set>
#include <vector>
#include <deque>
#include <map>
#include <regex>
#include <iostream>
#include <fstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <experimental/filesystem>
#include <thread>
#include <mutex>

using namespace std;
namespace fs = experimental::filesystem;

enum class FileStatus {no_access, dir, regfile, other};

template<typename T>
class TD;

FileStatus check_file (const char * file_name) noexcept {
    struct stat file_stat;
    if (stat (file_name, &file_stat) != 0) 
        return FileStatus::no_access;

    if (S_ISREG (file_stat.st_mode)) 
        return FileStatus::regfile;

    if (S_ISDIR (file_stat.st_mode))
        return FileStatus::dir;

    return FileStatus::other;
}

inline  FileStatus check_file (const string& file_name) noexcept {
    return check_file (file_name.c_str ());
} 
 
void check_new_vocabulary (const set<string>& vocab_dictionary, 
                           const string& new_file, 
                           map<string, unsigned>& new_vocabulary_count) {
    ifstream stream (new_file);
    if (stream.good ()) {
        regex pat {"[a-zA-Z][a-z]+"};
        sregex_token_iterator end {};
        string s;
        while (stream >> s) 
            for (sregex_token_iterator p{s.begin(),s.end(),pat}; p!=end; ++p) {
                string w {*p};
                w [0] = tolower (w [0]);
                vocab_dictionary.count (w) || new_vocabulary_count [w] ++;
            }
    }
    stream.close ();
}

class {
    mutable mutex m;
    public:
    auto& operator<<(const string& msg) const {
        lock_guard<mutex> g(m);
        cerr << msg << endl;
        return * this;
    }
} Logger;


void check_new_vocabulary (const set<string>& vocab_dictionary, 
                           const vector<string>& new_files, 
                           map<string, unsigned>& new_vocabulary_count) {
    deque<map<string, unsigned>> counts;
    vector<thread> threads;
    for (const auto& f : new_files) 
        threads.push_back (thread ([f, &counts, &vocab_dictionary](){
                counts.emplace_back ();
                Logger << string("Begin ") + f;
                check_new_vocabulary (vocab_dictionary, f, counts.back ());
                Logger << string ("End ") + f;
            }));

    for (auto& t : threads) {
        t.join ();
        const auto& f = counts.front ();
        for (const auto& [w, n]: f) new_vocabulary_count [w] += n;
        counts.pop_front ();
    }
}

void load_dictionary (const string& vocab_file, set<string>& vocab_dictionary) {
    ifstream vocab_stream (vocab_file);
    if (! vocab_stream.good ()) {
        cerr << vocab_file << " cannot be opened" << endl;
        abort ();
    }

    string word;
    while (vocab_stream >> word) 
        vocab_dictionary.insert (word);
    vocab_stream.close ();
}

int main (int argc, char * argv[]) {
    set<string> vocab_dictionary;
    vector<string> new_files;
    map<string, unsigned> new_vocabulary_count;

    option long_options[] {
        {"vocab", required_argument, nullptr, 'v'},
        {"new", required_argument, nullptr, 'n'},
        {nullptr,0,nullptr,0}
    };

    string vocab_file {};
    string new_file {};

    for (int optidx {0}, 
             opt {getopt_long (argc, argv, "v:n:", long_options, & optidx)};
         opt != -1;
         opt = getopt_long (argc, argv, "v:n:", long_options, & optidx)) 
        switch (opt) {
        case 'v':
            vocab_file = string (optarg);
            break;
        case 'n':
            new_file = string (optarg);
            break;
        default:
            abort ();
        }

    if (vocab_file.empty ()) {
        cerr << "No vocabulary file provided" << endl;
        abort ();
    }

    if (new_file.empty ()) {
        cerr << "No new files provided" << endl;
        abort ();
    }

    if (check_file (vocab_file) != FileStatus::regfile) {
        cerr << vocab_file 
             << " is not accessible or not a regular file" << endl;
        abort ();
    }

    switch (check_file (new_file)) {
    case FileStatus::dir:
        for (const auto & p : fs::directory_iterator (new_file.c_str())) {
            const auto path = p.path ();
            if (check_file (path) == FileStatus::regfile) 
                new_files.push_back (path);
        } 
        break;
    case FileStatus::regfile:
        new_files.push_back (new_file);
        break;

    default:
        cerr << "Cannot cope with " << new_file << endl;
        abort ();
    }

    load_dictionary (vocab_file, vocab_dictionary);
    //cout << vocab_dictionary.size () << " words founds" << endl;

    check_new_vocabulary (vocab_dictionary, new_files, new_vocabulary_count);

    for (const auto& [w,n] : new_vocabulary_count) {
        cout << w << " => " << n << endl;
    }
} 
