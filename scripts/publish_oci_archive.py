#!/usr/bin/env python
"""Publish a Docker 29+ OCI save archive to the native registry, without a container engine."""
import argparse, hashlib, json, ssl, tarfile
from urllib.parse import urljoin
import httpx

def publish(archive, registry, repository, tag, ca_cert=None):
    context=ssl.create_default_context(cafile=ca_cert)
    base=registry.rstrip('/')
    with tarfile.open(archive) as tar, httpx.Client(base_url=base,verify=context,trust_env=False,timeout=300) as client:
        index=json.load(tar.extractfile('index.json'))
        if len(index['manifests'])!=1:raise ValueError('Export one native image per archive')
        def read(d):
            raw=tar.extractfile('blobs/'+d['digest'].replace(':','/')).read()
            if len(raw)!=d['size'] or 'sha256:'+hashlib.sha256(raw).hexdigest()!=d['digest']:
                raise ValueError('OCI descriptor checksum mismatch')
            return raw
        def upload_blob(d):
            endpoint='/v2/'+repository+'/blobs/'+d['digest']
            head=client.head(endpoint)
            if head.status_code==200:return
            if head.status_code!=404:head.raise_for_status()
            path='blobs/'+d['digest'].replace(':','/')
            r=client.post('/v2/'+repository+'/blobs/uploads/');r.raise_for_status()
            target=urljoin(base,r.headers['Location'])
            if not target.startswith(base+'/'):raise ValueError('Registry upload left the configured origin')
            sha=hashlib.sha256()
            def chunks(stream):
                while block:=stream.read(1024*1024):
                    sha.update(block);yield block
            with tar.extractfile(path) as stream:
                r=client.put(target,params={'digest':d['digest']},content=chunks(stream),
                             headers={'Content-Type':'application/octet-stream','Content-Length':str(d['size'])})
            r.raise_for_status()
            if 'sha256:'+sha.hexdigest()!=d['digest']:raise ValueError('Uploaded blob checksum mismatch')
        def upload_manifest(d,reference):
            raw=read(d);manifest=json.loads(raw)
            if 'manifests' in manifest:
                for child in manifest['manifests']:upload_manifest(child,child['digest'])
            else:
                for blob in [manifest['config'],*manifest['layers']]:upload_blob(blob)
            response=client.put('/v2/'+repository+'/manifests/'+reference,content=raw,
                                headers={'Content-Type':d['mediaType']})
            response.raise_for_status()
            if response.headers.get('Docker-Content-Digest')!=d['digest']:raise ValueError('Registry manifest digest mismatch')
        descriptor=index['manifests'][0]
        upload_manifest(descriptor,tag)
        result={'reference':base.split('://',1)[1]+'/'+repository+'@'+descriptor['digest'],
                'digest':descriptor['digest'],'tag':tag,'tls_verified':True}
        print(json.dumps(result),flush=True)
        return result

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('archive');p.add_argument('registry');p.add_argument('repository');p.add_argument('tag');p.add_argument('--ca-cert')
    a=p.parse_args();publish(a.archive,a.registry,a.repository,a.tag,a.ca_cert)
