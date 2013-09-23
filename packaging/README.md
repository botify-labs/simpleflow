# Package builder for botify-cdf

This tools aims at building native Debian packages for botify-cdf dependencies, so botify-cdf itself can be packaged as well.

## Prerequisites

To be ran, that tools need:

* Vagrant
* Ansible
* An internet connection

A fresh checkout of this repository (or at least no existing VM since we need a fresh VM to start)

## Howto

Using botify-cdf is easy, just run:

```
vagrant up
```

Have a coffee break (it takes a loooong time to build as the system starts with a complete upgrade), the packages are in the current directory/packages


