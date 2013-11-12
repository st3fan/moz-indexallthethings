#!/usr/bin/env python

import xml.etree.ElementTree as etree
import pprint
import commands
import re
import sys

import dateutil.parser
import pymongo

pp = pprint.PrettyPrinter(indent=4)

def find_last_revision(repo_path):
    output = commands.getoutput("hg --repository %s summary" % repo_path)
    m = re.match(r"^parent:\s+(\d+):.*", output) #, flags=re.MULTILINE)
    if m is not None:
        return int(m.group(1))

def find_changed_files(repo_path, revision):
    output = commands.getoutput("hg --repository %s status --change %d" % (repo_path, revision))
    for line in output.split('\n'):
        e = line.split()
        if len(e) == 2:
            yield {'type': e[0], 'path': e[1]}

def _parse_logentry(entry):
    msg = entry.find('msg')
    tag = entry.find('tag')
    date = entry.find('date')
    author = entry.find('author')
    e = { 'revision': int(entry.attrib['revision']), 'node': entry.attrib['node'],
              'msg': msg.text, 'author': { 'name': author.text, 'email': author.attrib['email'] },
              'date': dateutil.parser.parse(date.text), 'paths': [], 'parents': [], 'type': 'change' }
    paths = entry.find('paths')
    if paths is not None:
        e['paths'] = []
        for path in paths.findall('path'):
            e['paths'].append({'action':path.attrib['action'], 'path':path.text})
    parents = [parent.attrib['node'] for parent in entry.findall('parent')]
    if parents is not None:
        e['parents'] = parents
        # TODO If there are multiple parents then this must be a merge. Is that correct?
        if len(parents) > 1:
            e['type'] = 'merge'
    if tag is not None:
        e['tag'] = tag.text
    return e

def hg_logentry(repo_path, revision):
    output = commands.getoutput("hg --repository %s log --style=xml -v -r %d" % (repo_path, revision))
    tree = etree.ElementTree(etree.fromstring(output))
    for entry in tree.getroot().findall('logentry'):
        yield _parse_logentry(entry)

FIRST_REVISION_IF_NO_DATA = 36802
#FIRST_REVISION_IF_NO_DATA = 0

if __name__ == "__main__":

    connection = pymongo.Connection()
    database = connection['bugzilla']
    commits = database['commits']

    repo_path = sys.argv[1]

    # Find the first revision that we dont have yet

    first_revision = 1

    last_commit = commits.find_one({},{"revision":1}, sort=[("revision", pymongo.DESCENDING)])
    if last_commit is not None:
        first_revision = last_commit['revision']

    last_revision = find_last_revision(repo_path)
    if not last_revision:
        print "Cannot get last revision of repository {}".format(repo_path)
        sys.exit(1)

    print "Fetching commits %d to %d" % (first_revision, last_revision)

    for revision in xrange(first_revision, last_revision + 1):
        logentries = hg_logentry(repo_path, revision)
        if logentries:
            for logentry in logentries:

                bug_id = None
                msg = logentry['msg'].split('\n')[0]
                match = re.match(r"^Bug (\d+)", msg, re.I)
                if match:
                    bug_id = int(match.group(1))

                if msg.endswith('.'):
                    msg = msg[:-1]

                reviewers = set()
                for group in re.findall("(r|sr|r\\+sr|r/sr|r\\+rs)=((?:[a-z0-9\\.\\@]+)(?:,[a-z0-9\\.\\@]+)*)", msg, re.I):
                    # Returns [('r', 'one,two'), ('r', 'justone'), ('r+sr', 'aa,bb,cc,dd'), ('r', 'a,b,c')]
                    if len(group) == 2:
                        for reviewer in group[1].split(","):
                            reviewers.add(reviewer)
                reviewers = list(reviewers)

                #match = re.match(r"(sr|r|r+sr)=([a-z0-9\._-]+(?:,[a-z0-9\._-]+)*)", msg)
                #if match is not None:
                #    for g in match.groups():
                #        print g
                #    #reviewers = match.group(1).split(r",\s*")

                print "%.10d %s %s" % (logentry['revision'], logentry['node'], msg)
                #print "   Reviewers: " + str(reviewers) + "\n"

                commit = logentry
                commit['_id'] = commit['node']

                if bug_id:
                    commit['bug_id'] = bug_id
                commit['reviewers'] = reviewers

                #pp.pprint(commit)
                commits.insert(commit)
