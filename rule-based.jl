using DataFrames
using StatsBase
using CSV
using Debugger
dbg = Debugger

# Selector
struct Selector    
    att::Symbol
    val
end
    function Base.show(io::IO, s::Selector)
        print(io, "($(s.att) = $(s.val))")
    end

# Rule

struct RuleBody
    selectors::Set{Selector}
end



function Base.:(==)(r1::RuleBody, r2::RuleBody)
    
    if ( length(r1.selectors) != length(r2.selectors) )
            return false
        end
        for r1sel in r1.selectors
            if r1sel ∉ r2.selectors
                return false
            end
        end
        return true
    end
    
    function Base.show(io::IO, r::RuleBody)
    
        nselector = length(r.selectors)
        count = 1
        for sel in r.selectors
            print("$sel")
            if count < nselector
                print(" ∧ ")
            end
            count = count + 1
        end
    end

    function getattributes(r::RuleBody)
        attributelist = []
        for sel in r.selectors
            push!(attributelist, sel.att)
        end
        return attributelist
    end
    function pushselector!(rule::RuleBody, selector::Selector)
        push!(rule.selectors, selector)
    end
    function appendselector(rule::RuleBody, s::Selector)
        newrule = deepcopy(rule)
        if s.att ∉ getattributes(rule)
            pushselector!(newrule, s)
        end
        return newrule
    end
    
    
    struct Rule
        body::RuleBody
        head::String
    end

    function Base.show(io::IO, rule::Rule)
        println(io, " $(rule.body) ⟶ ($(rule.head))" )
    end
    
    
# Star
struct Star 
    rules::Array{RuleBody}
end
    function Base.isempty(star::Star)
        return ( length(star.rules) == 0 )
    end

    function starsize(star::Star)
        return length(star.rules)
    end

    function pushrule!(star, rule)
        push!(star.rules, rule)
    end

    function rules2star(ruleslist::Array{RuleBody})
        star = Star(ruleslist)
        return star
    end

    function Base.show(io::IO, s::Star)
        println("Star:")
        for r in s.rules
            print(" • ")
            println(io,r)
        end
    end

    function specializestar( star, selectors )
        newstar = Star([])

        if isempty(star)
            for selector ∈ selectors
                newrule = RuleBody(Set([selector]))
                pushrule!(newstar, newrule)
            end        
        else
            for rule ∈ star.rules
                for selector ∈ selectors
                    newrule = appendselector(rule, selector)
                    if ( newrule ∉ newstar.rules ) && ( newrule != rule )
                        pushrule!(newstar, newrule)
                    end
                    
                end
            end
        end
        
        return newstar
    end

    function Base.show(io::IO, dict::Dict{RuleBody, Float32})
        for key ∈ keys(dict)
            println(io,"$key    $(dict[key])")
        end
    end

# end structures definition ____________________________________________________________________

function entropy(x)

    if length(x) == 0
        return 2.0    
    end
    val = values(countmap(x))
    if length(val) == 1
        return 0.0
    end    

    logbase = length(val)
    pi = val ./ sum(val)
    return -sum( pi .* log.(logbase, pi) )
end

function computeselectors(df)
    selectorlist = []
    attributes = names(df)
    # do per scontato che l' attributo target sia l' ultimo 
    for attribute in attributes[1:end-1]
        map( x -> push!( selectorlist, Selector(Symbol(attribute), x)), unique(df[:, attribute]) )
    end

    return selectorlist
end

function getrulecoverage(rule::RuleBody, examples)

    rulecoverage = ones(Bool, nrow(examples))

    for selector ∈ rule.selectors
        selcoverage = ( examples[!, selector.att] .== selector.val ) 
        rulecoverage = Bool.(selcoverage) .& rulecoverage
    end
    return rulecoverage
end 

function findBestComplex(selectors, examples)

        star = Star([])
        bestrule = RuleBody(Set([]))
        bestruleentropy = 2
        
        while true
            
            newstar = specializestar(star, selectors)
             #= Exit condition =#
             if isempty(newstar)
                break
            end
            #= Dataframe definition =#
            entropydf = DataFrame(R=RuleBody[], E=Float32[])

            for rule ∈  newstar.rules

                rulecoverage = getrulecoverage(rule, examples)
                # Indici delle istanze coperte
                coveredindexes = findall(x->x==1, rulecoverage)
                # Array cotenente gli attributi target di ogni istanza coperta
                coveredclasses = examples[ coveredindexes, end]
                enrpy = entropy(coveredclasses)
                # Aggiungo la regola e la sua valutazione al dizionario
                push!(entropydf, (rule, enrpy))
                
            end
           
            sort!(entropydf, [:E])
            @bp
            # Tende a mantenere le regole più generali a parità di entropia 
            newbestruleentropy = entropydf[1, :E]
            if ( newbestruleentropy < bestruleentropy)
                bestrule = entropydf[1, :R]
                bestruleentropy = entropydf[1, :E]
            end
            # Reduce de number of rules to the user defined max
            userdefinedmax = 3
            if nrow(entropydf) > userdefinedmax
                entropydf = entropydf[1:userdefinedmax, :]
            end
            newstarrules = entropydf[:, :R] 
            star = rules2star( newstarrules )
         end
    return bestrule
end

function getmostcommon( classlist )
    occurrence = countmap(classlist)
    return findmin(occurrence)[2]
end


function execute( filename ) 


    examples = CSV.read(filename, DataFrame; delim=',', ntasks=1)
    selectorlist = computeselectors(examples)

    #= RULE_LIST =#
    rulelist = []

    while nrow(examples) > 0

        # find the best rule for examples
        bestrule = findBestComplex(selectorlist, examples)
        
        # get the covered instance's indexes
        rulecoverage = getrulecoverage(bestrule, examples)
        coveredindexes = findall(x->x==1, rulecoverage)
        examplesclass = examples[coveredindexes, end]
        mostcommonclass = getmostcommon(examplesclass)
        # add the new rule to the rule list
        push!(rulelist, Rule(bestrule, mostcommonclass) ) 
        # remove covered from the examples
        deleteat!(examples, coveredindexes)
        
    end

    #= Output  =#
    println("Rules: ( ordered )")
    for rule ∈ rulelist
        print(" * ")
        print(rule)
    end
end

